use bytes::BytesMut;
pub use postgres_types::{FromSql, Type};
use postgres_types::{IsNull, ToSql};
use std::collections::HashMap;
use std::fmt::Display;
use std::io::{Read, Result, Write};

use client_message::{ClientMessage, Describe, FormatCode, PasswordMessage, StartupMessage};
use server_message::{CommandCompleteTag, ServerMessage};

mod client_message;
mod server_message;

pub struct PostgressIntermediary<Stream, Shim, PortalData> {
    stream: Stream,
    shim: Shim,
    portals: HashMap<String, Portal<PortalData>>,
}

pub trait PostgresShim<PortalData> {
    fn prepare(
        &mut self,
        query_name: String,
        query: String,
        parameter_types: Vec<Type>,
    ) -> Result<()>;
    fn bind(&mut self, query_name: String, parameters: Vec<ParameterValue>) -> Result<PortalData>;
    fn describe(&mut self, portal: &PortalData) -> Result<Option<Vec<Column>>>;
    fn execute<'a, S>(
        &mut self,
        portal: PortalData,
        max_rows: u32,
        columns: Option<Vec<Column>>,
        result_writer: ResultWriter<'a, S>,
    ) -> Result<()>
    where
        S: Write;
    fn default_parameters(&mut self) -> DefaultServerParameters;
}

pub struct Portal<PortalData> {
    portal_data: PortalData,
    columns: Option<Vec<Column>>,
    result_format_codes: Vec<FormatCode>,
}

pub struct ResultWriter<'a, S> {
    stream: &'a mut S,
    result_format_codes: Vec<FormatCode>,
}

pub struct RowWriter<'a, S> {
    stream: &'a mut S,
    result_format_codes: Vec<FormatCode>,
    types: Vec<Type>,
    row_count: u32,
}

pub struct DefaultServerParameters {
    pub server_version: String,
    pub server_encoding: String,
    pub client_encoding: String,
    pub application_name: String,
    pub default_transaction_read_only: String,
    pub in_hot_standby: String,
    pub is_superuser: String,
    pub session_authorization: String,
    pub date_style: String,
    pub interval_style: String,
    pub time_zone: String,
    pub integer_datetimes: String,
    pub standard_conforming_strings: String,
}

#[derive(Clone)]
pub struct Column {
    pub name: String,
    pub column_type: Type,
}

impl<'a, S> ResultWriter<'a, S> {
    fn new(result_format_codes: Vec<FormatCode>, stream: &'a mut S) -> Self {
        Self {
            result_format_codes,
            stream,
        }
    }

    pub fn start_writing<'b>(
        self,
        columns: impl IntoIterator<Item = &'b Column>,
    ) -> Result<RowWriter<'a, S>>
    where
        &'a mut S: Write,
    {
        let columns: Vec<Column> = columns.into_iter().cloned().collect();
        let format_codes = format_codes(&columns, self.result_format_codes.clone());
        Ok(RowWriter::new(
            format_codes,
            columns
                .iter()
                .map(|column| column.column_type.clone())
                .collect(),
            self.stream,
        ))
    }

    pub fn empty_result(mut self) -> Result<()>
    where
        &'a mut S: Write,
    {
        ServerMessage::EmptyQueryResponse.write(&mut self.stream)?;
        Ok(())
    }
}

fn row_description(
    columns: &Vec<Column>,
    result_format_codes: Vec<FormatCode>,
) -> Result<ServerMessage> {
    Ok(ServerMessage::RowDescription {
        fields: columns
            .iter()
            .zip(result_format_codes.clone())
            .map(|(column, format_code)| {
                (column.name.clone(), column.column_type.clone(), format_code)
            })
            .collect(),
    })
}

fn format_codes(columns: &Vec<Column>, result_format_codes: Vec<FormatCode>) -> Vec<FormatCode> {
    let format_codes = match result_format_codes.len() {
        0 => vec![FormatCode::Text; columns.len()],
        1 => vec![result_format_codes[0].clone(); columns.len()],
        _ => result_format_codes,
    };
    if format_codes.len() != columns.len() {
        panic!("Invalid number of columns compared to expected result format codes");
    }
    format_codes
}

impl<'a, S> RowWriter<'a, S>
where
    &'a mut S: Write,
{
    fn new(result_format_codes: Vec<FormatCode>, types: Vec<Type>, stream: &'a mut S) -> Self {
        Self {
            result_format_codes,
            stream,
            types,
            row_count: 0,
        }
    }

    pub fn write_row<I, E>(&mut self, rows: I) -> Result<()>
    where
        I: IntoIterator<Item = E>,
        E: ToSqlValue,
    {
        let fields: Vec<Option<BytesMut>> = rows
            .into_iter()
            .zip(&self.result_format_codes)
            .zip(&self.types)
            .map(|((sql_value, format_code), ty)| match format_code {
                FormatCode::Binary => sql_value.as_bin_value(ty),
                FormatCode::Text => sql_value.as_str_value(ty),
            })
            .collect();
        ServerMessage::DataRow { fields }.write(&mut self.stream)?;
        self.row_count += 1;
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        self.complete_result()?;
        Ok(())
    }

    fn complete_result(&mut self) -> Result<()> {
        ServerMessage::CommandComplete(CommandCompleteTag::Select {
            rows: self.row_count,
        })
        .write(&mut self.stream)?;
        Ok(())
    }
}

pub trait ToSqlValue: std::fmt::Debug {
    fn as_bin_value(&self, ty: &Type) -> Option<BytesMut>;
    fn as_str_value(&self, ty: &Type) -> Option<BytesMut>;
}

impl<T> ToSqlValue for T
where
    T: ToSql + Display,
{
    fn as_bin_value(&self, ty: &Type) -> Option<BytesMut> {
        let mut buffer = BytesMut::new();
        match self.to_sql(ty, &mut buffer) {
            Ok(IsNull::Yes) => None,
            Ok(IsNull::No) => Some(buffer),
            Err(_) => panic!("Error using type as bin postgres"),
        }
    }

    fn as_str_value(&self, _: &Type) -> Option<BytesMut> {
        let self_as_string = self.to_string();
        match self_as_string.as_str() {
            "NULL" => None,
            _ => Some(self_as_string.as_bytes().into()),
        }
    }
}

impl<PortalData> Portal<PortalData> {
    pub fn new(portal_data: PortalData, result_format_codes: Vec<FormatCode>) -> Self {
        Self {
            portal_data,
            columns: None,
            result_format_codes,
        }
    }

    pub fn add_columns(&mut self, columns: Option<Vec<Column>>) {
        self.columns = columns;
    }

    pub fn data(self) -> (PortalData, Option<Vec<Column>>) {
        (self.portal_data, self.columns)
    }
}

#[derive(Debug)]
pub enum ParameterValue {
    Text(String),
    Binary(Vec<u8>),
}

impl<Stream, Shim, PortalData> PostgressIntermediary<Stream, Shim, PortalData> {
    pub fn new(shim: Shim, stream: Stream) -> Self {
        Self {
            shim,
            stream,
            portals: HashMap::new(),
        }
    }

    pub fn run(mut self) -> std::io::Result<()>
    where
        Stream: Read + Write,
        Shim: PostgresShim<PortalData>,
    {
        self.init()?;
        loop {
            match ClientMessage::from_stream(&mut self.stream)? {
                ClientMessage::Parse {
                    name,
                    query,
                    parameters_types,
                } => {
                    self.shim.prepare(name, query, parameters_types)?;
                    ServerMessage::ParseComplete.write(&mut self.stream)?;
                }
                ClientMessage::Bind {
                    portal,
                    name,
                    parameter_format_codes,
                    parameters,
                    result_format_codes,
                } => {
                    let parameters = match parameter_format_codes.len() {
                        0 => parameters
                            .into_iter()
                            .map(|data| {
                                ParameterValue::Text(String::from_utf8_lossy(&data).to_string())
                            })
                            .collect(),
                        1 => parameters
                            .into_iter()
                            .map(|data| match parameter_format_codes[0] {
                                FormatCode::Text => {
                                    ParameterValue::Text(String::from_utf8_lossy(&data).to_string())
                                }
                                FormatCode::Binary => ParameterValue::Binary(data),
                            })
                            .collect(),
                        _ => parameters
                            .into_iter()
                            .zip(parameter_format_codes)
                            .map(|(data, format_code)| match format_code {
                                FormatCode::Text => {
                                    ParameterValue::Text(String::from_utf8_lossy(&data).to_string())
                                }
                                FormatCode::Binary => ParameterValue::Binary(data),
                            })
                            .collect(),
                    };
                    self.portals.insert(
                        portal,
                        Portal::new(self.shim.bind(name, parameters)?, result_format_codes),
                    );
                    ServerMessage::BindComplete.write(&mut self.stream)?;
                }
                ClientMessage::Execute { portal, max_rows } => match self.portals.remove(&portal) {
                    Some(portal) => {
                        let format_codes = portal.result_format_codes.clone();
                        let (data, columns) = portal.data();
                        self.shim.execute(
                            data,
                            max_rows,
                            columns,
                            ResultWriter::new(format_codes, &mut self.stream),
                        )?
                    }
                    None => {
                        ServerMessage::ErrorResponse {
                            code: 'S' as u8,
                            message: "Portal not found".to_string(),
                        }
                        .write(&mut self.stream)?;
                    }
                },
                ClientMessage::Query { query } => {
                    println!("{}", query)
                }
                ClientMessage::Describe(describe) => match describe {
                    Describe::Portal { name } => {
                        let portal = self.portals.get_mut(&name).unwrap();
                        match self.shim.describe(&portal.portal_data)? {
                            None => ServerMessage::NoData.write(&mut self.stream)?,
                            Some(columns) => {
                                row_description(
                                    &columns,
                                    format_codes(&columns, portal.result_format_codes.clone()),
                                )?
                                .write(&mut self.stream)?;
                                portal.add_columns(Some(columns));
                            }
                        }
                    }
                    Describe::Statement { name: _ } => {
                        todo!("We need to handle statement describe")
                    }
                },
                ClientMessage::Sync => {
                    ServerMessage::ReadyForQuery {
                        transaction_status: 'I' as u8,
                    }
                    .write(&mut self.stream)?;
                }
                ClientMessage::Terminate => {
                    return Ok(());
                }
            }
            self.stream.flush()?;
        }
    }

    fn init(&mut self) -> std::io::Result<()>
    where
        Stream: Read + Write,
        Shim: PostgresShim<PortalData>,
    {
        let _ = StartupMessage::from_stream(&mut self.stream)?;
        ServerMessage::AuthenticationCleartextPassword.write(&mut self.stream)?;
        self.stream.flush()?;
        let _ = PasswordMessage::from_stream(&mut self.stream)?;
        ServerMessage::AuthenticationOk.write(&mut self.stream)?;
        let default_parameters = self.shim.default_parameters();
        ServerMessage::ParameterStatus {
            name: "server_version",
            value: &default_parameters.server_version,
        }
        .write(&mut self.stream)?;
        ServerMessage::ParameterStatus {
            name: "server_encoding",
            value: &default_parameters.server_encoding,
        }
        .write(&mut self.stream)?;
        ServerMessage::ParameterStatus {
            name: "client_encoding",
            value: &default_parameters.client_encoding,
        }
        .write(&mut self.stream)?;
        ServerMessage::ParameterStatus {
            name: "application_name",
            value: &default_parameters.application_name,
        }
        .write(&mut self.stream)?;
        ServerMessage::ParameterStatus {
            name: "default_transaction_read_only",
            value: &default_parameters.default_transaction_read_only,
        }
        .write(&mut self.stream)?;
        ServerMessage::ParameterStatus {
            name: "in_hot_standby",
            value: &default_parameters.in_hot_standby,
        }
        .write(&mut self.stream)?;
        ServerMessage::ParameterStatus {
            name: "server_version",
            value: &default_parameters.server_version,
        }
        .write(&mut self.stream)?;
        ServerMessage::ParameterStatus {
            name: "is_superuser",
            value: &default_parameters.is_superuser,
        }
        .write(&mut self.stream)?;
        ServerMessage::ParameterStatus {
            name: "DateStyle",
            value: &default_parameters.date_style,
        }
        .write(&mut self.stream)?;
        ServerMessage::ParameterStatus {
            name: "IntervalStyle",
            value: &default_parameters.interval_style,
        }
        .write(&mut self.stream)?;
        ServerMessage::ParameterStatus {
            name: "TimeZone",
            value: &default_parameters.time_zone,
        }
        .write(&mut self.stream)?;
        ServerMessage::ParameterStatus {
            name: "integer_datetimes",
            value: &default_parameters.integer_datetimes,
        }
        .write(&mut self.stream)?;
        ServerMessage::ParameterStatus {
            name: "standard_conforming_strings",
            value: &default_parameters.standard_conforming_strings,
        }
        .write(&mut self.stream)?;
        ServerMessage::BackendKeyData {
            process_id: 0,
            secret_key: 0,
        }
        .write(&mut self.stream)?;
        ServerMessage::ReadyForQuery {
            transaction_status: 'I' as u8,
        }
        .write(&mut self.stream)?;
        self.stream.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
