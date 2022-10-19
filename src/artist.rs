use rusqlite::{params, Connection, Transaction};
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use std::fs::File;
use std::io::{BufRead, BufReader};

#[serde_as]
#[derive(Deserialize, Debug)]
struct Artist {
    #[serde_as(as = "DisplayFromStr")]
    id: u32,
    name: String,
    other_names: Vec<String>,
    // group_name: String,
    is_banned: Option<bool>,
    is_deleted: bool,
}

fn insert_data(filename: &str, tx: &Transaction) -> Result<(), anyhow::Error> {
    let mut artists_stmt = tx.prepare("INSERT INTO artists VALUES (?1, ?2, ?3, ?4, ?5, ?6)")?;
    let mut artist_other_names_stmt =
        tx.prepare("INSERT INTO artist_other_names VALUES (?1, ?2)")?;

    let file = File::open(filename)?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        let line: Artist = serde_json::from_str(&line)?;

        artists_stmt.execute(params![
            &line.id,
            &line.name,
            &line.is_banned,
            &line.is_deleted,
            0,
            0
        ])?;

        if line.other_names.len() > 0 {
            for name in line.other_names {
                artist_other_names_stmt.execute(params![&line.id, &name])?;
            }
        }
    }
    Ok(())
}

pub fn import_artist(filename: &str, conn: &mut Connection) -> Result<(), anyhow::Error> {
    conn.execute(
        "CREATE TABLE artists (
            artist_id   INT PRIMARY KEY,
            name        TEXT,
            is_banned   INT,
            is_deleted  INT,
            count       INT,
            tag_id      INT
        )",
        (),
    )?;
    conn.execute(
        "CREATE TABLE artist_other_names (
            artist_id   INT,
            name        TEXT,
            PRIMARY KEY(artist_id, name)
        )",
        (),
    )?;
    let tx = conn.transaction()?;
    insert_data(filename, &tx)?;
    tx.commit()?;
    Ok(())
}
