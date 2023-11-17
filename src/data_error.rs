use std::{error::Error, fmt::Display};

#[derive(Debug)]
pub enum PostgresDataError {
    PostgresError(tokio_postgres::error::Error),
    PostgresMapperError(tokio_pg_mapper::Error),
    ParseUuidError(uuid::Error),
    NoDataError(String),
    MissingIdError,
}

impl PostgresDataError {
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

impl Display for PostgresDataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PostgresDataError::PostgresError(e) => f.write_str(e.to_string().as_str()),
            PostgresDataError::PostgresMapperError(e) => f.write_str(e.to_string().as_str()),
            PostgresDataError::ParseUuidError(e) => f.write_str(e.to_string().as_str()),
            PostgresDataError::NoDataError(e) => f.write_str(&format!("No data found for {e}")),
            PostgresDataError::MissingIdError => f.write_str("Missing id"),
        }
    }
}

impl Error for PostgresDataError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {
            PostgresDataError::PostgresError(ref e) => Some(e),
            PostgresDataError::PostgresMapperError(ref e) => Some(e),
            PostgresDataError::ParseUuidError(ref e) => Some(e),
            PostgresDataError::NoDataError(_) => None,
            PostgresDataError::MissingIdError => None,
        }
    }
}

impl From<tokio_postgres::error::Error> for PostgresDataError {
    fn from(e: tokio_postgres::error::Error) -> Self {
        PostgresDataError::PostgresError(e)
    }
}

impl From<tokio_pg_mapper::Error> for PostgresDataError {
    fn from(e: tokio_pg_mapper::Error) -> Self {
        PostgresDataError::PostgresMapperError(e)
    }
}

impl From<uuid::Error> for PostgresDataError {
    fn from(e: uuid::Error) -> Self {
        PostgresDataError::ParseUuidError(e)
    }
}
