use rusqlite::{params, Connection, Transaction};
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};

#[serde_as]
#[derive(Deserialize, Debug)]
struct Pool {
    #[serde_as(as = "DisplayFromStr")]
    id: u32,
    name: String,
    description: String,
    post_ids: Vec<String>,
    is_active: bool,
    is_deleted: bool,
}

fn insert_data(filename: &str, tx: &Transaction) -> Result<(), anyhow::Error> {
    let mut pools_stmt = tx.prepare("INSERT INTO pools VALUES (?1, ?2, ?3, ?4)")?;
    let mut pool_images_stmt = tx.prepare("INSERT INTO pool_images VALUES (?1, ?2)")?;

    let file = File::open(filename)?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        let line: Pool = serde_json::from_str(&line)?;
        if line.is_deleted || !line.is_active {
            continue;
        }
        if line.post_ids.len() == 0 {
            continue;
        }
        pools_stmt.execute(params![
            &line.id,
            &line.name,
            &line.description,
            &line.post_ids.len()
        ])?;
        let uniq_post_ids: HashSet<String> = line.post_ids.into_iter().collect();
        for image_id in uniq_post_ids {
            pool_images_stmt.execute(params![&line.id, image_id])?;
        }
    }
    Ok(())
}

pub fn import_pool(filename: &str, conn: &mut Connection) -> Result<(), anyhow::Error> {
    conn.execute(
        "CREATE TABLE pools (
            pool_id         INT PRIMARY KEY,
            name            TEXT,
            desc            TEXT,
            image_count     INT
        )",
        (),
    )?;
    conn.execute(
        "CREATE TABLE pool_images (
            pool_id         INT,
            image_id        INT,
            PRIMARY KEY(pool_id, image_id)
        )",
        (),
    )?;

    let tx = conn.transaction()?;
    insert_data(filename, &tx)?;
    tx.commit()?;
    Ok(())
}
