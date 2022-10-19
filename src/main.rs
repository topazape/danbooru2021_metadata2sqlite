use glob::glob;
mod artist;
mod image;
mod note;
mod pool;
mod tag;
use clap::Parser;
use rusqlite::Connection;

#[derive(Parser)]
#[clap(name = "metadata2sqlite")]
struct AppArg {
    #[clap(short = 'd', long = "metadir")]
    metadir: String,
}

fn create_indices(conn: &Connection) -> Result<(), anyhow::Error> {
    // add useful indices when looking up images by tag
    conn.execute("CREATE INDEX tags_ids on tags(tag_id)", ())?;
    conn.execute("CREATE INDEX tags_dex on tags(name, tag_id)", ())?;
    conn.execute("CREATE INDEX image_ids on images(image_id)", ())?;
    conn.execute("CREATE INDEX artist_ids on artists(artist_id)", ())?;
    conn.execute("CREATE INDEX artist_tag on artists(tag_id)", ())?;

    // useful index for looking up image by rating
    conn.execute("CREATE INDEX image_rate on images(image_id, rating)", ())?;

    Ok(())
}

fn make_image_tags(conn: &Connection) -> Result<(), anyhow::Error> {
    conn.execute(
        "UPDATE itTemp set tag_id = (SELECT tag_id FROM tags WHERE tags.name = itTemp.tag AND tags.category = itTemp.cat)", ()
    )?;
    conn.execute(
        "INSERT INTO imageTags SELECT image_id, tag_id FROM itTemp",
        (),
    )?;

    conn.execute("CREATE INDEX image_tags ON imageTags(tag_id)", ())?;
    Ok(())
}

fn update_counts(conn: &Connection) -> Result<(), anyhow::Error> {
    conn.execute(
        "UPDATE tags set count = (SELECT COUNT(image_id) FROM imagetags WHERE imagetags.tag_id = tags.tag_id)
        WHERE EXISTS (SELECT * FROM imagetags WHERE imagetags.tag_id = tags.tag_id)", ()
    )?;
    conn.execute(
        "UPDATE ARTISTS set count = (SELECT count(image_id) FROM imagetags WHERE imagetags.tag_id = artists.tag_id)
        WHERE EXISTS (SELECT * FROM imagetags WHERE imagetags.tag_id = artists.tag_id)", ()
    )?;
    conn.execute(
        "UPDATE artists set tag_id = (select tag_id FROM tags WHERE artists.name = tags.name AND tags.category=1)", ()
    )?;

    Ok(())
}

fn main() -> Result<(), anyhow::Error> {
    let arg: AppArg = AppArg::parse();
    let metadata_dir = std::path::Path::new(&arg.metadir);
    assert_eq!(metadata_dir.is_dir(), true);

    let path = "danbooru2021.db";
    let mut conn = Connection::open(path)?;

    image::create_image_tables(&mut conn)?;
    for entry in glob(
        metadata_dir
            .join("posts*.json")
            .as_os_str()
            .to_str()
            .unwrap(),
    )? {
        let entry = entry?;
        if let Some(filename) = entry.into_os_string().to_str() {
            println!("{}", filename);
            image::import_image(filename, &mut conn)?;
        }
    }

    for entry in glob(
        metadata_dir
            .join("tags*.json")
            .as_os_str()
            .to_str()
            .unwrap(),
    )? {
        let entry = entry?;
        if let Some(filename) = entry.into_os_string().to_str() {
            println!("{}", filename);
            tag::import_tag(filename, &mut conn)?;
        }
    }

    for entry in glob(
        metadata_dir
            .join("notes*.json")
            .as_os_str()
            .to_str()
            .unwrap(),
    )? {
        let entry = entry?;
        if let Some(filename) = entry.into_os_string().to_str() {
            println!("{}", filename);
            note::import_note(filename, &mut conn)?;
        }
    }

    for entry in glob(
        metadata_dir
            .join("pools*.json")
            .as_os_str()
            .to_str()
            .unwrap(),
    )? {
        let entry = entry?;
        if let Some(filename) = entry.into_os_string().to_str() {
            println!("{}", filename);
            pool::import_pool(filename, &mut conn)?;
        }
    }

    for entry in glob(
        metadata_dir
            .join("artists*.json")
            .as_os_str()
            .to_str()
            .unwrap(),
    )? {
        let entry = entry?;
        if let Some(filename) = entry.into_os_string().to_str() {
            println!("{}", filename);
            artist::import_artist(filename, &mut conn)?;
        }
    }

    println!("{}", "indices");
    create_indices(&conn)?;

    println!("{}", "imageTags");
    make_image_tags(&conn)?;

    println!("{}", "counts");
    update_counts(&conn)?;

    println!("{}", "cleanup");
    conn.execute("DROP TABLE itTemp", ())?;
    conn.execute("VACUUM", ())?;

    conn.close().unwrap();

    Ok(())
}
