use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::serve;
use axum::Router;
use dotenv::dotenv;
use openssl::ssl::{SslConnector, SslMethod};
use polars::prelude::*;
use tokio_postgres::connect;
use tokio_postgres::types::Type;
use tokio_postgres::Client;
use postgres_openssl::MakeTlsConnector;
use std::env;
use std::error;

async fn db_client() -> Result<Client, Box<dyn error::Error>> {
    dotenv().ok();
    let conn_str = env::var("DATABASE_URL")?;

    let builder = SslConnector::builder(SslMethod::tls())?;
    let connector = MakeTlsConnector::new(builder.build());
    let (client, connection) = connect(&conn_str, connector).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    Ok(client)
}

fn to_polars_type(postgres_type: &Type) -> DataType {
    match postgres_type.oid() {
        21 => DataType::Int16,
        23 => DataType::Int32,
        20 => DataType::Int64,
        25 => DataType::String,
        700 => DataType::Float32,
        701 => DataType::Float64,
        _ => todo!(),
    }
}

fn lazyframe_from_sql(query: &Vec<tokio_postgres::Row>) -> Result<LazyFrame, Box<dyn error::Error>> {
    // Work out the schema of our LazyFrame based on the first row of the query result
    let schema = if let Some(row) = query.get(0) {
        let mut schema = Schema::with_capacity(row.columns().len());
        for col in row.columns() {
            schema.insert(col.name().into(), to_polars_type(col.type_()));
        }
        schema
    } else {
        return Err("Query returned no rows".into());
    };

    // Convert datatypes and add to column-wise buffers
    let mut col_buffers = vec![Vec::with_capacity(query.len()); schema.len()];
    query.iter().for_each(|row| {
        // I'm in a fun lifetimes situation here where `for` doesn't let `row` live long enough but `for_each` does
        for (i, (name, dtype)) in schema.iter().enumerate() {
            let datum = match dtype {
                DataType::Int16 => AnyValue::Int16(row.get::<&str, i16>(name.as_str())),
                DataType::Int32 => AnyValue::Int32(row.get::<&str, i32>(name.as_str())),
                DataType::Int64 => AnyValue::Int64(row.get::<&str, i64>(name.as_str())),
                DataType::String => AnyValue::String(row.get::<&str, &str>(name.as_str())),
                DataType::Float32 => AnyValue::Float32(row.get::<&str, f32>(name.as_str())),
                DataType::Float64 => AnyValue::Float64(row.get::<&str, f64>(name.as_str())),
                _ => unreachable!(),
            };
            col_buffers[i].push(datum);
        }
    });

    // Convert buffers to polars Columns and create LazyFrame
    let cols = col_buffers
        .iter()
        .zip(schema.iter_names())
        .map(|(col, name)| Column::new(name.clone(), col))
        .collect::<Vec<_>>();
    Ok(DataFrame::new(cols)?.lazy())
}

async fn handle_print_frame(State(app_state): State<AppState>) -> Result<String, StatusCode> {
    let query = app_state.client
        .query("select * from playing_with_neon", &[])
        .await
        .map_err(|_e| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(lazyframe_from_sql(&query)
        .map_err(|_e| StatusCode::INTERNAL_SERVER_ERROR)?
        .collect()
        .map_err(|_e| StatusCode::INTERNAL_SERVER_ERROR)?
        .to_string())
}

#[derive(Clone)]
struct AppState {
    client: Arc<Client>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let client = Arc::new(db_client().await?);
    let state = AppState { client };
    let app = Router::new()
        .route("/print_lazyframe", get(handle_print_frame))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    serve(listener, app).await?;

    Ok(())
}
