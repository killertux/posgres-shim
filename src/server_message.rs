use bytes::BytesMut;
use postgres_types::Type;
use std::io::{Cursor, Result, Write};

use crate::client_message::FormatCode;

#[derive(Debug)]
pub enum ServerMessage<'a> {
    AuthenticationOk,
    AuthenticationCleartextPassword,
    BackendKeyData {
        process_id: i32,
        secret_key: i32,
    },
    BindComplete,
    CommandComplete(CommandCompleteTag),
    DataRow {
        fields: Vec<Option<BytesMut>>,
    },
    ErrorResponse {
        code: u8,
        message: String,
    },
    EmptyQueryResponse,
    NoData,
    ParameterStatus {
        name: &'a str,
        value: &'a str,
    },
    ParseComplete,
    ReadyForQuery {
        transaction_status: u8,
    },
    RowDescription {
        fields: Vec<(String, Type, FormatCode)>,
    },
}

#[derive(Debug)]
pub enum CommandCompleteTag {
    Select { rows: u32 },
}

impl<'a> ServerMessage<'a> {
    pub fn write(self, stream: &mut impl WritePostgresExt) -> Result<()> {
        match self {
            Self::AuthenticationOk => {
                stream.write(&['R' as u8])?;
                stream.write_int32(8)?;
                stream.write_int32(0)?;
            }
            Self::BackendKeyData {
                process_id,
                secret_key,
            } => {
                stream.write_byte('K' as u8)?;
                stream.write_int32(12)?;
                stream.write_int32(process_id)?;
                stream.write_int32(secret_key)?;
            }
            Self::ReadyForQuery { transaction_status } => {
                stream.write_byte('Z' as u8)?;
                stream.write_int32(5)?;
                stream.write(&[transaction_status])?;
            }
            Self::AuthenticationCleartextPassword => {
                stream.write_byte('R' as u8)?;
                stream.write_int32(8)?;
                stream.write_int32(3)?;
            }
            Self::ParameterStatus { name, value } => {
                stream.write_byte('S' as u8)?;
                stream.write_int32((4 + name.len() + 1 + value.len() + 1) as i32)?;
                stream.write_all(&name.as_bytes())?;
                stream.write_byte(0)?;
                stream.write_all(&value.as_bytes())?;
                stream.write_byte(0)?;
            }
            Self::ParseComplete => {
                stream.write_byte('1' as u8)?;
                stream.write_int32(4)?;
            }
            Self::BindComplete => {
                stream.write_byte('2' as u8)?;
                stream.write_int32(4)?;
            }
            Self::ErrorResponse { code, message } => {
                stream.write_byte('E' as u8)?;
                stream.write_int32((message.len() + 4 + 1 + 1) as i32)?;
                stream.write_byte(code)?;
                stream.write_all(&message.as_bytes())?;
                stream.write_byte(0)?;
            }
            Self::RowDescription { fields } => {
                stream.write_byte('T' as u8)?;
                let mut buffer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
                buffer.write_int16(fields.len() as u16)?;
                fields
                    .iter()
                    .map(|(field_name, field_type, field_format)| -> Result<()> {
                        buffer.write_all(&field_name.as_bytes())?;
                        buffer.write_byte(0)?;
                        buffer.write_int32(0)?;
                        buffer.write_int16(0)?;
                        buffer.write_int32(field_type.oid() as i32)?;
                        buffer.write_int16(0)?;
                        buffer.write_int32(0)?;
                        buffer.write_int16(match field_format {
                            FormatCode::Text => 0,
                            FormatCode::Binary => 1,
                        })?;
                        Ok(())
                    })
                    .collect::<Result<()>>()?;
                let buffer = buffer.into_inner();
                stream.write_int32(buffer.len() as i32 + 4)?;
                stream.write_all(&buffer)?;
            }
            Self::DataRow { fields } => {
                stream.write_byte('D' as u8)?;
                let mut buffer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
                buffer.write_int16(fields.len() as u16)?;
                for field in fields {
                    match field {
                        None => buffer.write_int32(-1)?,
                        Some(data) => {
                            buffer.write_int32(data.len() as i32)?;
                            buffer.write_all(&data)?;
                        }
                    }
                }
                let buffer = buffer.into_inner();
                stream.write_int32(buffer.len() as i32 + 4)?;
                stream.write_all(&buffer)?;
            }
            Self::CommandComplete(command_complete) => {
                stream.write_byte('C' as u8)?;
                let mut buffer: Cursor<Vec<u8>> = Cursor::new(Vec::new());
                match command_complete {
                    CommandCompleteTag::Select { rows } => {
                        buffer.write_all(format!("SELECT {}", rows).as_bytes())?;
                    }
                }
                buffer.write_byte(0)?;
                let buffer = buffer.into_inner();
                stream.write_int32(buffer.len() as i32 + 4)?;
                stream.write_all(&buffer)?;
            }
            Self::NoData => {
                stream.write_byte('n' as u8)?;
                stream.write_int32(4)?;
            }
            Self::EmptyQueryResponse => {
                stream.write_byte('I' as u8)?;
                stream.write_int32(4)?;
            }
        }
        Ok(())
    }
}

pub trait WritePostgresExt: Write {
    fn write_int32(&mut self, number: i32) -> Result<()> {
        self.write_all(&number.to_be_bytes())?;
        Ok(())
    }

    fn write_int16(&mut self, number: u16) -> Result<()> {
        self.write_all(&number.to_be_bytes())?;
        Ok(())
    }

    fn write_byte(&mut self, byte: u8) -> Result<()> {
        self.write(&[byte])?;
        Ok(())
    }
}
impl<T> WritePostgresExt for T where T: Write {}
