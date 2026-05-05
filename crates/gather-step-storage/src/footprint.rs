use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use rusqlite::{Connection, OpenFlags};
use serde::Serialize;
use thiserror::Error;

use crate::{GraphStoreDb, GraphStoreError, GraphTableFootprint};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StorageFootprintReport {
    pub storage_root: String,
    pub total_bytes: u64,
    pub components: Vec<StorageFootprintComponent>,
    pub graph_tables: Vec<GraphTableFootprint>,
    pub sqlite_objects: Vec<SqliteObjectFootprint>,
    pub search_files: Vec<StorageFileFootprint>,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StorageFootprintComponent {
    pub name: String,
    pub store_kind: String,
    pub path: String,
    pub bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SqliteObjectFootprint {
    pub name: String,
    pub object_type: String,
    pub bytes: u64,
    pub pages: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StorageFileFootprint {
    pub path: String,
    pub category: String,
    pub bytes: u64,
}

#[derive(Debug, Error)]
pub enum StorageFootprintError {
    #[error("failed to access storage files: {0}")]
    Io(#[from] std::io::Error),
    #[error("graph store error: {0}")]
    Graph(#[from] GraphStoreError),
    #[error("sqlite metadata store error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

pub fn storage_footprint_report(
    root: impl AsRef<Path>,
) -> Result<StorageFootprintReport, StorageFootprintError> {
    let root = root.as_ref();
    let mut warnings = Vec::new();
    let mut components = Vec::new();
    let mut search_files = Vec::new();

    let total_bytes = directory_size_bytes(root)?;
    let graph_path = root.join("graph.redb");
    let search_path = root.join("search");
    let metadata_path = root.join("metadata.sqlite");

    components.push(StorageFootprintComponent {
        name: "graph".to_owned(),
        store_kind: "redb".to_owned(),
        path: relative_path(root, &graph_path),
        bytes: file_size_bytes(&graph_path)?,
    });
    components.push(StorageFootprintComponent {
        name: "metadata".to_owned(),
        store_kind: "sqlite".to_owned(),
        path: relative_path(root, &metadata_path),
        bytes: sqlite_file_family_bytes(&metadata_path)?,
    });
    collect_file_footprints(root, &search_path, &mut search_files)?;
    components.push(StorageFootprintComponent {
        name: "search".to_owned(),
        store_kind: "tantivy".to_owned(),
        path: relative_path(root, &search_path),
        bytes: directory_size_bytes(&search_path)?,
    });

    let graph_tables = if graph_path.exists() {
        GraphStoreDb::open(&graph_path)?.table_footprints()?
    } else {
        warnings.push(format!("graph store not found at {}", graph_path.display()));
        Vec::new()
    };

    let sqlite_objects = if metadata_path.exists() {
        sqlite_object_footprints(&metadata_path, &mut warnings)?
    } else {
        warnings.push(format!(
            "metadata store not found at {}",
            metadata_path.display()
        ));
        Vec::new()
    };

    components.sort_by(|left, right| {
        right
            .bytes
            .cmp(&left.bytes)
            .then_with(|| left.name.cmp(&right.name))
    });
    search_files.sort_by(|left, right| {
        right
            .bytes
            .cmp(&left.bytes)
            .then_with(|| left.path.cmp(&right.path))
    });

    Ok(StorageFootprintReport {
        storage_root: root.display().to_string(),
        total_bytes,
        components,
        graph_tables,
        sqlite_objects,
        search_files,
        warnings,
    })
}

fn sqlite_object_footprints(
    path: &Path,
    warnings: &mut Vec<String>,
) -> Result<Vec<SqliteObjectFootprint>, StorageFootprintError> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let object_types = sqlite_object_types(&connection)?;
    let mut statement = match connection.prepare(
        "SELECT name, SUM(pgsize) AS bytes, COUNT(*) AS pages \
         FROM dbstat GROUP BY name ORDER BY bytes DESC, name ASC",
    ) {
        Ok(statement) => statement,
        Err(error) => {
            warnings.push(format!("sqlite dbstat unavailable: {error}"));
            return Ok(Vec::new());
        }
    };
    let rows = statement.query_map([], |row| {
        let name: String = row.get(0)?;
        let bytes: i64 = row.get(1)?;
        let pages: i64 = row.get(2)?;
        Ok(SqliteObjectFootprint {
            object_type: object_types
                .get(&name)
                .cloned()
                .unwrap_or_else(|| "internal".to_owned()),
            name,
            bytes: u64::try_from(bytes).unwrap_or(0),
            pages: u64::try_from(pages).unwrap_or(0),
        })
    })?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(StorageFootprintError::from)
}

fn sqlite_object_types(
    connection: &Connection,
) -> Result<BTreeMap<String, String>, StorageFootprintError> {
    let mut object_types = BTreeMap::new();
    let mut statement =
        connection.prepare("SELECT name, type FROM sqlite_schema WHERE name IS NOT NULL")?;
    let rows = statement.query_map([], |row| {
        let name: String = row.get(0)?;
        let object_type: String = row.get(1)?;
        Ok((name, object_type))
    })?;
    for row in rows {
        let (name, object_type) = row?;
        object_types.insert(name, object_type);
    }
    object_types.insert("sqlite_schema".to_owned(), "schema".to_owned());
    Ok(object_types)
}

fn sqlite_file_family_bytes(path: &Path) -> Result<u64, std::io::Error> {
    let mut total = file_size_bytes(path)?;
    for suffix in ["-wal", "-shm"] {
        total = total.saturating_add(file_size_bytes(&PathBuf::from(format!(
            "{}{suffix}",
            path.display()
        )))?);
    }
    Ok(total)
}

fn collect_file_footprints(
    root: &Path,
    path: &Path,
    files: &mut Vec<StorageFileFootprint>,
) -> Result<u64, std::io::Error> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    if metadata.file_type().is_symlink() {
        return Ok(0);
    }
    if metadata.is_file() {
        let bytes = metadata.len();
        files.push(StorageFileFootprint {
            path: relative_path(root, path),
            category: classify_storage_file(path),
            bytes,
        });
        return Ok(bytes);
    }
    if !metadata.is_dir() {
        return Ok(0);
    }

    let mut total = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        total = total.saturating_add(collect_file_footprints(root, &entry.path(), files)?);
    }
    Ok(total)
}

fn directory_size_bytes(path: &Path) -> Result<u64, std::io::Error> {
    let mut files = Vec::new();
    collect_file_footprints(path, path, &mut files)
}

fn file_size_bytes(path: &Path) -> Result<u64, std::io::Error> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(metadata.len()),
        Ok(_) => Ok(0),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error),
    }
}

fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .display()
        .to_string()
}

fn classify_storage_file(path: &Path) -> String {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    if file_name == "meta.json" {
        return "tantivy_meta".to_owned();
    }
    if file_name.contains("schema-version") {
        return "schema_version".to_owned();
    }
    match path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or_default()
    {
        "idx" => "tantivy_index".to_owned(),
        "term" => "tantivy_terms".to_owned(),
        "pos" => "tantivy_positions".to_owned(),
        "store" => "tantivy_stored_fields".to_owned(),
        "fast" => "tantivy_fast_fields".to_owned(),
        "fieldnorm" => "tantivy_fieldnorms".to_owned(),
        "json" => "json_metadata".to_owned(),
        other if !other.is_empty() => other.to_owned(),
        _ => "unknown".to_owned(),
    }
}
