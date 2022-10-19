use rusqlite::{params, Connection, Transaction};
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use std::fs::File;
use std::io::{BufRead, BufReader};

#[serde_as]
#[derive(Deserialize, Debug)]
struct Note {
    #[serde_as(as = "DisplayFromStr")]
    id: u32,
    #[serde_as(as = "Option<DisplayFromStr>")]
    image_id: Option<u32>,
    body: String,
    #[serde_as(as = "Option<DisplayFromStr>")]
    x: Option<u32>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    y: Option<u32>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    w: Option<u32>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    h: Option<u32>,
    is_active: bool,
}

fn insert_data(filename: &str, tx: &Transaction) -> Result<(), anyhow::Error> {
    let mut stmt = tx.prepare("INSERT INTO notes VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)")?;

    let file = File::open(filename)?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        let line: Note = serde_json::from_str(&line)?;
        if !line.is_active {
            continue;
        }
        stmt.execute(params![
            &line.id,
            &line.image_id,
            &line.body,
            &line.x,
            &line.y,
            &line.w,
            &line.h
        ])?;
    }
    Ok(())
}

pub fn import_note(filename: &str, conn: &mut Connection) -> Result<(), anyhow::Error> {
    conn.execute(
        "CREATE TABLE notes (
            note_id     INT PRIMARY KEY,
            image_id    INT,
            body        TEXT,
            x           INT,
            y           INT,
            w           INT,
            h           INT
        )",
        (),
    )?;
    let tx = conn.transaction()?;
    insert_data(filename, &tx)?;
    tx.commit()?;
    Ok(())
}
