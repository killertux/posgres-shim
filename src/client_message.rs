use postgres_types::Type;
use std::collections::HashMap;
use std::io::{Cursor, Read, Result};

#[derive(Debug)]
pub struct StartupMessage {
    pub protocol_version: u32,
    pub user: String,
    pub database: Option<String>,
    pub options: Option<String>,
    pub replication: Option<String>,
    pub parameters: HashMap<String, String>,
}

#[derive(Debug)]
pub struct PasswordMessage {
    pub password: String,
}

#[derive(Debug)]
pub enum ClientMessage {
    Query {
        query: String,
    },
    Parse {
        name: String,
        query: String,
        parameters_types: Vec<Type>,
    },
    Bind {
        portal: String,
        name: String,
        parameter_format_codes: Vec<FormatCode>,
        parameters: Vec<Vec<u8>>,
        result_format_codes: Vec<FormatCode>,
    },
    Execute {
        portal: String,
        max_rows: u32,
    },
    Describe(Describe),
    Sync,
    Terminate,
}

#[derive(Debug, Clone)]
pub enum Describe {
    Statement { name: String },
    Portal { name: String },
}

#[derive(Debug, Clone)]
pub enum FormatCode {
    Text,
    Binary,
}

impl PasswordMessage {
    pub fn from_stream(stream: &mut impl ReadPostgresExt) -> Result<Self> {
        let header = stream.read_byte()?;
        if header != 'p' as u8 {
            panic!("Invalid message");
        }
        let lenght_of_bytes = stream.read_int32()?;
        let mut buffer = vec![0; lenght_of_bytes as usize - 4];
        stream.read_exact(&mut buffer)?;
        Ok(PasswordMessage {
            password: String::from_utf8_lossy(&buffer[..buffer.len() - 1]).to_string(),
        })
    }
}

impl StartupMessage {
    pub fn from_stream(stream: &mut impl ReadPostgresExt) -> Result<Self> {
        let lenght_of_bytes = stream.read_int32()?;
        let protocol_version = stream.read_int32()?;
        let mut buffer = vec![0; lenght_of_bytes as usize - 4 - 4];
        stream.read_exact(&mut buffer)?;
        let mut i = 0;
        let mut parameters = HashMap::new();
        let mut user = String::new();
        let mut database = None;
        let mut options = None;
        let mut replication = None;

        while match buffer.get(i) {
            Some(0) | None => false,
            _ => true,
        } {
            let parameter_name = read_string(&buffer, &mut i);
            let parameter_value = read_string(&buffer, &mut i);
            match parameter_name.as_str() {
                "user" => user = parameter_value,
                "database" => database = Some(parameter_value),
                "options" => options = Some(parameter_value),
                "replication" => replication = Some(parameter_value),
                _ => {
                    parameters.insert(parameter_name, parameter_value);
                }
            };
        }
        Ok(Self {
            protocol_version,
            user,
            database,
            options,
            replication,
            parameters,
        })
    }
}

impl ClientMessage {
    pub fn from_stream(stream: &mut impl ReadPostgresExt) -> Result<Self> {
        let type_identification = stream.read_byte()?;
        match type_identification as char {
            'Q' => {
                let lenght = stream.read_int32()?;
                let mut buffer = vec![0; lenght as usize - 4];
                stream.read_exact(&mut buffer)?;
                Ok(Self::Query {
                    query: String::from_utf8_lossy(&buffer[..buffer.len() - 1]).to_string(),
                })
            }
            'P' => {
                let lenght = stream.read_int32()?;
                let mut buffer = vec![0; lenght as usize - 4];
                stream.read_exact(&mut buffer)?;
                let mut i = 0;
                let name = read_string(&buffer, &mut i);
                let query = read_string(&buffer, &mut i);
                let mut cursor = Cursor::new(&buffer[i..]);
                let n_parameters = cursor.read_int16()?;
                let mut parameters_types: Vec<Type> = Vec::new();
                for _ in 0..n_parameters {
                    parameters_types.push(Type::from_oid(cursor.read_int32()?).unwrap());
                }
                Ok(Self::Parse {
                    name,
                    query,
                    parameters_types,
                })
            }
            'B' => {
                let lenght = stream.read_int32()?;
                let mut buffer = vec![0; lenght as usize - 4];
                stream.read_exact(&mut buffer)?;
                let mut i = 0;
                let portal = read_string(&buffer, &mut i);
                let name = read_string(&buffer, &mut i);
                let mut cursor = Cursor::new(&buffer[i..]);
                let n_format_codes = cursor.read_int16()?;
                let parameter_format_codes = (0..n_format_codes)
                    .into_iter()
                    .map(|_| match cursor.read_int16() {
                        Ok(0) => FormatCode::Text,
                        Ok(1) => FormatCode::Binary,
                        _ => panic!("Invalid format"),
                    })
                    .collect::<Vec<FormatCode>>();
                let n_parameters = cursor.read_int16()?;
                let mut parameters = Vec::new();
                for _ in 0..n_parameters {
                    let parameter_size = cursor.read_int32()?;
                    let mut buffer = vec![0; parameter_size as usize];
                    cursor.read_exact(&mut buffer)?;
                    parameters.push(buffer);
                }
                let n_result_format_codes = cursor.read_int16()?;
                let result_format_codes = (0..n_result_format_codes)
                    .into_iter()
                    .map(|_| match cursor.read_int16() {
                        Ok(0) => FormatCode::Text,
                        Ok(1) => FormatCode::Binary,
                        _ => panic!("Invalid format"),
                    })
                    .collect::<Vec<FormatCode>>();

                Ok(Self::Bind {
                    portal,
                    name,
                    parameter_format_codes,
                    parameters,
                    result_format_codes,
                })
            }
            'E' => {
                let lenght = stream.read_int32()?;
                let mut buffer = vec![0; lenght as usize - 4];
                stream.read_exact(&mut buffer)?;
                let mut i = 0;
                let portal = read_string(&buffer, &mut i);
                let mut cursor = Cursor::new(&buffer[i..]);
                let max_rows = cursor.read_int32()?;
                Ok(Self::Execute { portal, max_rows })
            }
            'D' => {
                let lenght = stream.read_int32()?;
                let describe_type = stream.read_byte()?;
                let mut buffer = vec![0; lenght as usize - 4 - 1];
                stream.read_exact(&mut buffer)?;
                let name = String::from_utf8_lossy(&buffer[..buffer.len() - 1]).to_string();
                Ok(Self::Describe(match describe_type as char {
                    'S' => Describe::Statement { name },
                    'P' => Describe::Portal { name },
                    _ => panic!("Invalid descripe type"),
                }))
            }
            'S' => {
                let _ = stream.read_int32()?;
                Ok(Self::Sync)
            }
            'X' => {
                let _ = stream.read_int32()?;
                Ok(Self::Terminate)
            }
            _ => panic!("Cannot process {}", type_identification as char),
        }
    }
}

pub trait ReadPostgresExt: Read {
    fn read_byte(&mut self) -> Result<u8> {
        let mut buf = [0; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn read_int32(&mut self) -> Result<u32> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf)?;
        Ok(u32::from_be_bytes(buf))
    }

    fn read_int16(&mut self) -> Result<u16> {
        let mut buf = [0; 2];
        self.read_exact(&mut buf)?;
        Ok(u16::from_be_bytes(buf))
    }
}
impl<T> ReadPostgresExt for T where T: Read {}

fn read_string(buffer: &[u8], start: &mut usize) -> String {
    let (mut end_of_string, _) = buffer[*start..]
        .iter()
        .enumerate()
        .find(|c| *c.1 == 0)
        .unwrap();
    end_of_string = *start + end_of_string;
    let result = String::from_utf8_lossy(&buffer[*start..end_of_string]).to_string();
    *start = end_of_string + 1;
    result
}
