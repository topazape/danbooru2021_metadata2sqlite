use rusqlite::{params, Connection, Transaction};
use serde::Deserialize;
use serde_with::{serde_as, DeserializeAs, DisplayFromStr};
use std::fs::File;
use std::io::{BufRead, BufReader};

#[serde_as]
#[derive(Deserialize, Debug)]
struct Post {
    #[serde_as(as = "Option<DisplayFromStr>")]
    id: Option<u32>,
    rating: String,
    source: String,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pixiv_id: Option<u32>,
    #[serde_as(as = "DisplayFromStr")]
    image_width: i32,
    #[serde_as(as = "DisplayFromStr")]
    image_height: i32,
    #[serde_as(as = "DisplayFromStr")]
    created_at: String,
    #[serde_as(as = "DisplayFromStr")]
    updated_at: String,
    #[serde_as(as = "DisplayFromStr")]
    uploader_id: u32,
    is_banned: bool,
    is_deleted: bool,
    is_flagged: bool,
    file_ext: String,
    #[serde_as(as = "DisplayFromStr")]
    file_size: u32,
    md5: Option<String>,
    has_children: bool,
    has_visible_children: bool,
    has_active_children: bool,
    #[serde_as(as = "Option<DisplayFromStr>")]
    parent_id: Option<u32>,
    #[serde_as(as = "ImgTag")]
    tag_string_character: Vec<String>, // 4
    #[serde_as(as = "ImgTag")]
    tag_string_copyright: Vec<String>, // 3
    #[serde_as(as = "ImgTag")]
    tag_string_meta: Vec<String>, // 5
    #[serde_as(as = "ImgTag")]
    tag_string_general: Vec<String>, // 0
    #[serde_as(as = "ImgTag")]
    tag_string_artist: Vec<String>, // 1
}

struct ImgTag;
impl<'de> DeserializeAs<'de, Vec<String>> for ImgTag {
    fn deserialize_as<D>(deserializer: D) -> Result<Vec<String>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let tag_str = String::deserialize(deserializer)?;

        let mut arr: Vec<String> = vec![];
        for tag in tag_str.split_whitespace() {
            arr.push(tag.to_string());
        }
        Ok(arr)
    }
}

pub fn create_image_tables(conn: &mut Connection) -> Result<(), anyhow::Error> {
    conn.execute(
        "CREATE TABLE images (
            image_id                INT PRIMARY KEY,
            rating                  TEXT,
            source                  TEXT,
            pixiv_id                TEXT,
            w                       INT,
            h                       INT,
            created_at              TEXT,
            updated_at              TEXT,
            uploader_id             INT,
            is_banned               INT,
            is_deleted              INT,
            is_flagged              INT,
            file_ext                TEXT,
            file_size               INT,
            md5                     TEXT,
            has_children            INT,
            has_visible_children    INT,
            has_active_children     INT,
            parent_id               INT
        )",
        (),
    )?;
    conn.execute(
        "CREATE TABLE itTemp (
            tag         TEXT,
            cat         INT,
            image_id    INT,
            tag_id      INT,
            PRIMARY KEY(tag, image_id, cat)
        )",
        (),
    )?;
    conn.execute(
        "CREATE TABLE imageTags (
            image_id    INT,
            tag_id      INT,
            PRIMARY KEY(image_id, tag_id)
        )",
        (),
    )?;
    Ok(())
}

fn insert_data(filename: &str, tx: &Transaction) -> Result<(), anyhow::Error> {
    let mut images_stmt = tx.prepare("INSERT INTO images VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)")?;
    let mut ittemp_stmt = tx.prepare("INSERT INTO itTemp VALUES (?1, ?2, ?3, ?4)")?;

    let file = File::open(filename)?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        let line: Post = serde_json::from_str(&line)?;
        if line.id.is_none() {
            continue;
        }
        images_stmt.execute(params![
            &line.id,
            &line.rating,
            &line.source,
            &line.pixiv_id,
            &line.image_width,
            &line.image_height,
            &line.created_at,
            &line.updated_at,
            &line.uploader_id,
            &line.is_banned,
            &line.is_deleted,
            &line.is_flagged,
            &line.file_ext,
            &line.file_size,
            &line.md5,
            &line.has_children,
            &line.has_visible_children,
            &line.has_active_children,
            &line.parent_id
        ])?;
        for character in line.tag_string_character {
            ittemp_stmt.execute(params![character, 4, &line.id, 0])?;
        }
        for copyright in line.tag_string_copyright {
            ittemp_stmt.execute(params![copyright, 3, &line.id, 0])?;
        }
        for meta in line.tag_string_meta {
            ittemp_stmt.execute(params![meta, 5, &line.id, 0])?;
        }
        for general in line.tag_string_general {
            ittemp_stmt.execute(params![general, 0, &line.id, 0])?;
        }
        for artist in line.tag_string_artist {
            ittemp_stmt.execute(params![artist, 1, &line.id, 0])?;
        }
    }
    Ok(())
}

pub fn import_image(filename: &str, conn: &mut Connection) -> Result<(), anyhow::Error> {
    let tx = conn.transaction()?;
    insert_data(filename, &tx)?;
    tx.commit()?;
    Ok(())
}
