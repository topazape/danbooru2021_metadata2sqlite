use rusqlite::{params, Connection, Transaction};
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use std::fs::File;
use std::io::{BufRead, BufReader};

#[serde_as]
#[derive(Deserialize, Debug)]
struct Tag {
    #[serde_as(as = "DisplayFromStr")]
    id: u32,
    name: String,
    #[serde_as(as = "DisplayFromStr")]
    category: u32,
    // #[serde_as(as = "Option<DisplayFromStr>")]
    // is_active: Option<u32>,
    #[serde_as(as = "DisplayFromStr")]
    post_count: u32,
}

fn insert_data(filename: &str, tx: &Transaction) -> Result<(), anyhow::Error> {
    let mut stmt = tx.prepare("INSERT INTO tags VALUES (?1, ?2, ?3, ?4)")?;

    let file = File::open(filename)?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        let line: Tag = serde_json::from_str(&line)?;
        if line.post_count == 0 {
            continue;
        }
        // note the count is set to zero because 'post_count' above isn't the real count
        stmt.execute(params![&line.id, &line.name, &line.category, 0])?;
    }
    Ok(())
}

pub fn import_tag(filename: &str, conn: &mut Connection) -> Result<(), anyhow::Error> {
    conn.execute(
        "CREATE TABLE tags (
            tag_id      INT PRIMARY KEY,
            name        TEXT,
            category    INT,
            count       INT
        )",
        (),
    )?;
    let tx = conn.transaction()?;
    insert_data(filename, &tx)?;
    tx.commit()?;
    Ok(())
}
