use std::{error::Error, fmt::Display};

#[derive(Debug)]
pub enum CrudyError {
    PostgresError(tokio_postgres::error::Error),
    PostgresMapperError(tokio_pg_mapper::Error),
    ParseUuidError(uuid::Error),
    NoDataError(String),
    MissingIdError,
}

impl CrudyError {
    pub fn new_no_data_error<S>(reference: &str, id: &S) -> Self
    where
        S: Display,
    {
        let message = format!("{reference} - {}", id);
        Self::NoDataError(message)
    }

    pub fn new_no_data_error_2<S>(reference: &str, id1: &S, id2: &S) -> Self
    where
        S: Display,
    {
        let message = format!("{reference} - {id1} {id2}");
        Self::NoDataError(message)
    }
}

impl Display for CrudyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CrudyError::PostgresError(e) => f.write_str(e.to_string().as_str()),
            CrudyError::PostgresMapperError(e) => f.write_str(e.to_string().as_str()),
            CrudyError::ParseUuidError(e) => f.write_str(e.to_string().as_str()),
            CrudyError::NoDataError(e) => f.write_str(&format!("No data found for {e}")),
            CrudyError::MissingIdError => f.write_str("Missing id"),
        }
    }
}

impl Error for CrudyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {
            CrudyError::PostgresError(ref e) => Some(e),
            CrudyError::PostgresMapperError(ref e) => Some(e),
            CrudyError::ParseUuidError(ref e) => Some(e),
            CrudyError::NoDataError(_) => None,
            CrudyError::MissingIdError => None,
        }
    }
}

impl From<tokio_postgres::error::Error> for CrudyError {
    fn from(e: tokio_postgres::error::Error) -> Self {
        CrudyError::PostgresError(e)
    }
}

impl From<tokio_pg_mapper::Error> for CrudyError {
    fn from(e: tokio_pg_mapper::Error) -> Self {
        CrudyError::PostgresMapperError(e)
    }
}

impl From<uuid::Error> for CrudyError {
    fn from(e: uuid::Error) -> Self {
        CrudyError::ParseUuidError(e)
    }
}
