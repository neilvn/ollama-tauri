use rusqlite::{Connection, Result};
use duckdb::{params, Connection as DuckdbConn};

#[derive(Debug)]
struct ResponseChunk {
    id: i64,
    response_id: String,
    chunk_data: Option<String>,
    sequence_num: i32,
    created_at: String
}

fn main() -> Result<(), rusqlite::Error> {
    let connection_result = Connection::open("path/to/file.db");
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
                    chunk_data: row.get::<_, Option<String>>(3)?,
                    created_at: row.get(4)?,
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

            let first = chunks.first().ok_or_else(||
                rusqlite::Error::InvalidParameterName("Missing start chunk".to_string()))?;

            let last = chunks.last().ok_or_else(||
                rusqlite::Error::InvalidParameterName("Missing end chunk".to_string()))?;

            let combined_data = chunks
                .iter()
                .filter_map(|chunk| chunk.chunk_data.clone())
                .collect::<Vec<String>>()
                .join("")
                .to_string();

            let duckdb_entry = DuckDbEntry {
                response_id: last.response_id.clone(),
                started_at: first.created_at.clone(),
                ended_at: last.created_at.clone(),
                answer: combined_data.clone(),
            };

            if let Err(e) = save_to_duckdb(duckdb_entry) {
                eprintln!("DuckDB error: {:?}", e);
            }

            println!("Combined data: {}", combined_data);
            Ok(())
        }

        Err(e) => {
            eprintln!("Error opening database: {}", e);
            Err(e)
        }
    }
}


struct DuckDbEntry {
    answer: String,
    response_id: String,
    started_at: String,
    ended_at: String
}

fn save_to_duckdb(entry: DuckDbEntry) -> Result<(), duckdb::Error> {
    let conn = DuckdbConn::open("entries.db")?;

    conn.execute("
        CREATE TABLE IF NOT EXISTS entries (
            id UUID PRIMARY KEY,
            answer TEXT NOT NULL,
            started_at STING NOT NULL,
            ended_AT STING NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ", [])?;

    conn.execute("
        INSERT INTO entries (id, answer, started_at, ended_at, created_at)
        VALUES (?, ?, ?, ?)",
        params![entry.response_id, entry.answer, entry.started_at, entry.ended_at]
    )?;

    Ok(())
}
