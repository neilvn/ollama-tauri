use rusqlite::{Connection, Result};
use duckdb::{Connection as DuckdbConn, Result as DuckdbResult, params};

#[allow(dead_code)]
#[derive(Debug)]
struct ResponseChunk {
    id: i64,
    response_id: String,
    chunk_data: Option<String>,
    sequence_num: i32,
}

fn main() -> Result<(), rusqlite::Error> {
    let connection_result = Connection::open("/Users/nevinod/Desktop/responses.db");
    let column = "response_id";
    let value = "6cd2398b-e050-47f1-bf3f-5b5b5b5f07c9";
    let table_name = "response_chunks";

    match connection_result {
        Ok(conn) => {
            let query = format!("SELECT * FROM {} WHERE {} = ?", table_name, column);
            let mut statement = conn.prepare(&query)?;

            let rows = statement.query_map(&[&value], |row| {
                Ok(ResponseChunk {
                    id: row.get(0)?,
                    response_id: row.get(1)?,
                    sequence_num: row.get(2)?,
                    chunk_data: row.get::<_, Option<String>>(3)? 
                })
            })?;

            let mut chunks = Vec::new();

            for result in rows {
                match result {
                    Ok(chunk) => chunks.push(chunk),
                    Err(e) => eprintln!("Error processing row: {}", e),
                }
            }

            chunks.sort_by_key(|chunk| chunk.sequence_num);

            let combined_data = chunks
                .iter()
                .filter_map(|chunk| chunk.chunk_data.clone())
                .collect::<Vec<String>>()
                .join("");

            println!("Combined data: {}", combined_data);

            Ok(())
        }
        Err(e) => {
            eprintln!("Error opening database: {}", e);
            Err(e)
        }
    }
}
