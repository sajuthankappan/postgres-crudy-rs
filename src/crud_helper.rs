use std::fmt::Display;

use tokio_pg_mapper::FromTokioPostgresRow;
use tokio_postgres::{types::ToSql, GenericClient, Row, ToStatement};

use crate::crudy_error::CrudyError;

#[derive(Debug, Clone)]
pub struct CrudHelper {
    pub schema: String,
}

impl CrudHelper {
    pub fn new() -> Self {
        CrudHelper {
            schema: "public".into(),
        }
    }

    pub fn with_schema<S: ToString>(schema: S) -> Self {
        CrudHelper {
            schema: schema.to_string(),
        }
    }

    pub async fn get_all<T, Client>(&self, client: &Client) -> Result<Vec<T>, CrudyError>
    where
        T: FromTokioPostgresRow,
        Client: GenericClient,
    {
        let query = format!(
            "SELECT * FROM \"{schema}\".{table}",
            schema = self.schema,
            table = T::sql_table()
        );
        let items = self.query(client, &query, &[]).await?;
        Ok(items)
    }

    pub async fn get_all_ordered<T, Client>(
        &self,
        client: &Client,
        order_by: &str,
    ) -> Result<Vec<T>, CrudyError>
    where
        T: FromTokioPostgresRow,
        Client: GenericClient,
    {
        let query = format!(
            "SELECT * FROM \"{schema}\".{table} ORDER BY {order_by}",
            schema = self.schema,
            table = T::sql_table()
        );
        let items = self.query(client, &query, &[]).await?;
        Ok(items)
    }

    pub async fn get_first<T, Client>(&self, client: &Client) -> Result<Option<T>, CrudyError>
    where
        T: FromTokioPostgresRow,
        Client: GenericClient,
    {
        let query = format!(
            "SELECT * FROM \"{schema}\".{table} LIMIT 1",
            schema = self.schema,
            table = T::sql_table()
        );
        let row = client.query_opt(query.as_str(), &[]).await?;

        if let Some(row) = row {
            let item = T::from_row(row)?;
            Ok(Some(item))
        } else {
            Ok(None)
        }
    }

    pub async fn get_many_by<T, U, Client>(
        &self,
        client: &Client,
        by_field: &str,
        value: &U,
        order_by: Option<&str>,
    ) -> Result<Vec<T>, CrudyError>
    where
        T: FromTokioPostgresRow,
        U: ToSql + Sync + Send,
        Client: GenericClient,
    {
        self.query_by(client, "*", by_field, value, order_by).await
    }

    pub async fn get_many_by_2<T, U, V, Client>(
        &self,
        client: &Client,
        by_field_1: &str,
        value_1: &U,
        by_field_2: &str,
        value_2: &V,
        order_by: Option<&str>,
    ) -> Result<Vec<T>, CrudyError>
    where
        T: FromTokioPostgresRow,
        U: ToSql + Sync + Send,
        V: ToSql + Sync + Send,
        Client: GenericClient,
    {
        self.query_by_2(
            client, "*", by_field_1, value_1, by_field_2, value_2, order_by,
        )
        .await
    }

    pub async fn get_by_str_id<T, Client>(
        &self,
        client: &Client,
        id: &str,
    ) -> Result<Option<T>, CrudyError>
    where
        T: FromTokioPostgresRow,
        Client: GenericClient,
    {
        let id = uuid::Uuid::parse_str(id)?;
        self.get_by(client, "id", &id).await
    }

    pub async fn get<T, I, Client>(&self, client: &Client, id: &I) -> Result<Option<T>, CrudyError>
    where
        T: FromTokioPostgresRow,
        I: ToSql + Sync + Send,
        Client: GenericClient,
    {
        self.get_by(client, "id", &id).await
    }

    pub async fn get_one<T, I, Client>(&self, client: &Client, id: &I) -> Result<T, CrudyError>
    where
        T: FromTokioPostgresRow,
        I: ToSql + Sync + Send + Display,
        Client: GenericClient,
    {
        self.get_by(client, "id", &id)
            .await?
            .ok_or(CrudyError::new_no_data_error(&T::sql_table(), id))
    }

    pub async fn get_specific_fields<T, I, Client>(
        &self,
        client: &Client,
        id: &I,
        fields: &str,
    ) -> Result<Option<T>, CrudyError>
    where
        T: FromTokioPostgresRow,
        I: ToSql + Sync + Send,
        Client: GenericClient,
    {
        self.get_specific_fields_by(client, "id", fields, &id).await
    }

    pub async fn get_by<T, U, Client>(
        &self,
        client: &Client,
        field: &str,
        value: &U,
    ) -> Result<Option<T>, CrudyError>
    where
        T: FromTokioPostgresRow,
        U: ToSql + Sync + Send,
        Client: GenericClient,
    {
        self.query_opt_by(client, "*", field, value).await
    }

    pub async fn get_by_2<T, U, V, Client>(
        &self,
        client: &Client,
        field_1: &str,
        value_1: &U,
        field_2: &str,
        value_2: &V,
    ) -> Result<Option<T>, CrudyError>
    where
        T: FromTokioPostgresRow,
        U: ToSql + Sync + Send,
        V: ToSql + Sync + Send,
        Client: GenericClient,
    {
        self.query_opt_by_2(client, "*", field_1, value_1, field_2, value_2)
            .await
    }

    pub async fn get_by_3<T, U, V, W, Client>(
        &self,
        client: &Client,
        field_1: &str,
        value_1: &U,
        field_2: &str,
        value_2: &V,
        field_3: &str,
        value_3: &W,
    ) -> Result<Option<T>, CrudyError>
    where
        T: FromTokioPostgresRow,
        U: ToSql + Sync + Send,
        V: ToSql + Sync + Send,
        W: ToSql + Sync + Send,
        Client: GenericClient,
    {
        self.query_opt_by_3(
            client, "*", field_1, value_1, field_2, value_2, field_3, value_3,
        )
        .await
    }

    pub async fn get_specific_fields_by<T, U, Client>(
        &self,
        client: &Client,
        fields: &str,
        field: &str,
        value: &U,
    ) -> Result<Option<T>, CrudyError>
    where
        T: FromTokioPostgresRow,
        U: ToSql + Sync + Send,
        Client: GenericClient,
    {
        self.query_opt_by(client, fields, field, value).await
    }

    async fn query_opt_by<T, U, Client>(
        &self,
        client: &Client,
        fields: &str,
        by_field: &str,
        value: &U,
    ) -> Result<Option<T>, CrudyError>
    where
        T: FromTokioPostgresRow,
        U: ToSql + Sync + Send,
        Client: GenericClient,
    {
        let query = format!(
            "SELECT {fields} FROM \"{schema}\".{table} WHERE {by_field} = $1",
            schema = self.schema,
            table = T::sql_table(),
            fields = fields,
            by_field = by_field,
        );

        self.query_opt(client, &query, &[&value]).await
    }

    async fn query_opt_by_2<T, U, V, Client>(
        &self,
        client: &Client,
        fields: &str,
        by_field_1: &str,
        value_1: &U,
        by_field_2: &str,
        value_2: &V,
    ) -> Result<Option<T>, CrudyError>
    where
        T: FromTokioPostgresRow,
        U: ToSql + Sync + Send,
        V: ToSql + Sync + Send,
        Client: GenericClient,
    {
        let query = format!(
                "SELECT {fields} FROM \"{schema}\".{table} WHERE {by_field_1} = $1 and {by_field_2} = $2",
                schema = self.schema,
                table = T::sql_table(),
                fields = fields,
            );

        self.query_opt(client, &query, &[&value_1, &value_2]).await
    }

    async fn query_opt_by_3<T, U, V, W, Client>(
        &self,
        client: &Client,
        fields: &str,
        by_field_1: &str,
        value_1: &U,
        by_field_2: &str,
        value_2: &V,
        by_field_3: &str,
        value_3: &W,
    ) -> Result<Option<T>, CrudyError>
    where
        T: FromTokioPostgresRow,
        U: ToSql + Sync + Send,
        V: ToSql + Sync + Send,
        W: ToSql + Sync + Send,
        Client: GenericClient,
    {
        let query = format!(
                "SELECT {fields} FROM \"{schema}\".{table} WHERE {by_field_1} = $1 and {by_field_2} = $2 and {by_field_3} = $3",
                schema = self.schema,
                table = T::sql_table(),
                fields = fields,
            );

        self.query_opt(client, &query, &[&value_1, &value_2, &value_3])
            .await
    }

    async fn query_by<T, U, Client>(
        &self,
        client: &Client,
        fields: &str,
        by_field: &str,
        value: &U,
        order_by: Option<&str>,
    ) -> Result<Vec<T>, CrudyError>
    // TODO: Rename to get_many
    where
        T: FromTokioPostgresRow,
        U: ToSql + Sync + Send,
        Client: GenericClient,
    {
        let order_by_clause = if let Some(order_by) = order_by {
            format!("ORDER BY {order_by}")
        } else {
            String::default()
        };

        let query = format!(
            "SELECT {fields} FROM \"{schema}\".{table} WHERE {by_field} = $1 {order_by_clause}",
            schema = self.schema,
            table = T::sql_table(),
            fields = fields,
            by_field = by_field,
        );
        self.query(client, &query, &[&value]).await
    }

    async fn query_by_2<T, U, V, Client>(
        &self,
        client: &Client,
        fields: &str,
        by_field_1: &str,
        value_1: &U,
        by_field_2: &str,
        value_2: &V,
        order_by: Option<&str>,
    ) -> Result<Vec<T>, CrudyError>
    // TODO: Rename to get_many
    where
        T: FromTokioPostgresRow,
        U: ToSql + Sync + Send,
        V: ToSql + Sync + Send,
        Client: GenericClient,
    {
        let order_by_clause = if let Some(order_by) = order_by {
            format!("ORDER BY {order_by}")
        } else {
            String::default()
        };

        let query = format!(
            "
                SELECT {fields} FROM \"{schema}\".{table} 
                WHERE
                    {by_field_1} = $1 AND
                    {by_field_2} = $2 
                {order_by_clause}
                ",
            schema = self.schema,
            table = T::sql_table(),
            fields = fields,
            by_field_1 = by_field_1,
            by_field_2 = by_field_2,
        );
        self.query(client, &query, &[&value_1, &value_2]).await
    }

    pub async fn query<T, U, Client>(
        &self,
        client: &Client,
        query: &U,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<T>, CrudyError>
    where
        T: FromTokioPostgresRow,
        U: ?Sized + ToStatement + Sync + Send,
        Client: GenericClient,
    {
        let rows = client.query(query, params).await?;
        let items = Self::map_rows(rows)?;
        Ok(items)
    }

    pub async fn query_opt<T, U, Client>(
        &self,
        client: &Client,
        query: &U,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<T>, CrudyError>
    where
        T: FromTokioPostgresRow,
        U: ?Sized + ToStatement + Sync + Send,
        Client: GenericClient,
    {
        let row = client.query_opt(query, params).await?;

        if let Some(row) = row {
            let item = T::from_row(row)?;
            Ok(Some(item))
        } else {
            Ok(None)
        }
    }

    pub async fn delete<T, I, Client>(&self, client: &Client, id: &I) -> Result<u64, CrudyError>
    where
        T: FromTokioPostgresRow,
        I: ToSql + Sync + Send,
        Client: GenericClient,
    {
        let query = format!(
            "DELETE FROM \"{schema}\".{table}
                 WHERE id = $1",
            schema = self.schema,
            table = T::sql_table(),
        );
        let updated = self.execute(client, &query, &[&id]).await?;
        Ok(updated)
    }

    pub async fn execute<T, Client>(
        &self,
        client: &Client,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, CrudyError>
    where
        T: ?Sized + ToStatement + Send + Sync,
        Client: GenericClient,
    {
        let rows = client.execute(statement, params).await?;
        Ok(rows)
    }

    pub async fn execute_for_id<T, U, I, Client>(
        &self,
        client: &Client,
        id: &I,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<U, CrudyError>
    where
        T: ?Sized + ToStatement + Send + Sync,
        U: FromTokioPostgresRow,
        I: ToSql + Sync + Send + ToString,
        Client: GenericClient,
    {
        let rows = client.execute(statement, params).await?;

        if rows == 0 {
            todo!("Number of rows affected is zero");
        }

        let item = self
            .get::<U, I, Client>(client, id)
            .await?
            .ok_or(CrudyError::NoDataError(id.to_string()))?;
        Ok(item)
    }

    fn map_rows<T>(rows: Vec<Row>) -> Result<Vec<T>, tokio_pg_mapper::Error>
    where
        T: FromTokioPostgresRow,
    {
        rows.into_iter().map(|row| T::from_row(row)).collect()
    }
}

impl Default for CrudHelper {
    fn default() -> Self {
        Self::new()
    }
}
