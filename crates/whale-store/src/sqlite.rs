use crate::backend::{validate_create, validate_replace};
use crate::{Result, RetentionMetadata, SessionRecord, SessionStore, StoreError};
use async_trait::async_trait;
use fs2::FileExt;
use rusqlite::{params, Connection, OptionalExtension};
use std::{
    fs::{File, OpenOptions},
    path::Path,
    sync::{Arc, Mutex},
};
struct Database {
    connection: Mutex<Connection>,
    _lock: File,
}
#[derive(Clone)]
pub struct SQLiteStore {
    database: Arc<Database>,
}
fn io(error: impl std::fmt::Display) -> StoreError {
    StoreError::Io(error.to_string())
}
impl SQLiteStore {
    /// Holds a canonical-path sidecar lock for the complete connection lifetime,
    /// including accepted blocking jobs. The sidecar is never unlinked: deleting
    /// it would let competing processes lock different inodes under the same name.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if path.as_os_str() == ":memory:" {
            return Err(StoreError::Invalid(
                "Use MemoryStore for memory-only storage".into(),
            ));
        }
        let path = if path.exists() {
            std::fs::canonicalize(path).map_err(io)?
        } else {
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            std::fs::canonicalize(parent).map_err(io)?.join(
                path.file_name()
                    .ok_or_else(|| StoreError::Invalid("Database path has no filename".into()))?,
            )
        };
        #[cfg(unix)]
        if path.exists() {
            use std::os::unix::fs::MetadataExt;
            if std::fs::metadata(&path).map_err(io)?.nlink() != 1 {
                return Err(StoreError::Invalid(
                    "Hard-linked SQLite database paths are unsupported".into(),
                ));
            }
        }
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".whale-lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)
            .map_err(io)?;
        lock.try_lock_exclusive()
            .map_err(|_| StoreError::Io("Store is already locked by another writer".into()))?;
        let connection = Connection::open(path).map_err(io)?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(io)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(io)?;
        connection.execute_batch("CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY NOT NULL, revision INTEGER NOT NULL, record TEXT NOT NULL);").map_err(io)?;
        Ok(Self {
            database: Arc::new(Database {
                connection: Mutex::new(connection),
                _lock: lock,
            }),
        })
    }
    async fn blocking<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let database = self.database.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = database
                .connection
                .lock()
                .map_err(|_| StoreError::Io("Database mutex poisoned".into()))?;
            operation(&mut connection)
        })
        .await
        .map_err(io)?
    }
}
#[async_trait]
impl SessionStore for SQLiteStore {
    fn durable(&self) -> bool {
        true
    }
    async fn create(&self, record: SessionRecord) -> Result<()> {
        validate_create(&record)?;
        self.blocking(move |db| {
            let tx = db.transaction().map_err(io)?;
            let existing: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?1)",
                    [&record.recovery_id],
                    |row| row.get(0),
                )
                .map_err(io)?;
            if existing {
                return Err(StoreError::Conflict);
            }
            tx.execute(
                "INSERT INTO sessions (id,revision,record) VALUES (?1,?2,?3)",
                params![
                    record.recovery_id,
                    i64::try_from(record.revision).map_err(io)?,
                    serde_json::to_string(&record).map_err(io)?
                ],
            )
            .map_err(io)?;
            tx.commit().map_err(io)
        })
        .await
    }
    async fn load(&self, id: &str) -> Result<Option<SessionRecord>> {
        let id = id.to_owned();
        self.blocking(move |db| {
            let text: Option<String> = db
                .query_row("SELECT record FROM sessions WHERE id=?1", [id], |row| {
                    row.get(0)
                })
                .optional()
                .map_err(io)?;
            text.map(|text| serde_json::from_str(&text).map_err(io))
                .transpose()
        })
        .await
    }
    async fn compare_exchange(
        &self,
        id: &str,
        expected: u64,
        replacement: SessionRecord,
    ) -> Result<()> {
        let id = id.to_owned();
        self.blocking(move |db| {
            let tx = db.transaction().map_err(io)?;
            let old: Option<String> = tx
                .query_row("SELECT record FROM sessions WHERE id=?1", [&id], |row| {
                    row.get(0)
                })
                .optional()
                .map_err(io)?;
            let old: SessionRecord =
                serde_json::from_str(&old.ok_or(StoreError::NotFound)?).map_err(io)?;
            validate_replace(&old, &id, expected, &replacement)?;
            let changed = tx
                .execute(
                    "UPDATE sessions SET revision=?1,record=?2 WHERE id=?3 AND revision=?4",
                    params![
                        i64::try_from(replacement.revision).map_err(io)?,
                        serde_json::to_string(&replacement).map_err(io)?,
                        id,
                        i64::try_from(expected).map_err(io)?
                    ],
                )
                .map_err(io)?;
            if changed != 1 {
                return Err(StoreError::Conflict);
            }
            tx.commit().map_err(io)
        })
        .await
    }
    async fn metadata_page(
        &self,
        after_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RetentionMetadata>> {
        let after = after_id.map(str::to_owned);
        let limit = i64::try_from(limit).map_err(io)?;
        self.blocking(move |db| {
            // JSON1 extracts only metadata; full model/history blobs are never
            // materialized as Rust SessionRecords during a retention scan.
            let mut stmt=db.prepare("SELECT id, revision,
                record -> '$.detached_since_ms', json_extract(record,'$.forgotten'),
                (json_extract(record,'$.owner') IS NOT NULL OR EXISTS(SELECT 1 FROM json_each(record,'$.runs') WHERE json_extract(value,'$.snapshot.status') NOT IN ('completed','failed','cancelled'))),
                EXISTS(SELECT 1 FROM json_each(record,'$.unknown_executions') WHERE json_extract(value,'$.acknowledged') = 0),
                CASE WHEN json_extract(record,'$.forgotten') THEN 0 ELSE length(CAST(record AS BLOB)) END
                FROM sessions WHERE (?1 IS NULL OR id > ?1) ORDER BY id LIMIT ?2").map_err(io)?;
            let rows=stmt.query_map(params![after,limit],|row|Ok((
                row.get::<_,String>(0)?,row.get::<_,i64>(1)?,row.get::<_,Option<String>>(2)?,row.get::<_,bool>(3)?,
                row.get::<_,bool>(4)?,row.get::<_,bool>(5)?,row.get::<_,i64>(6)?,
            ))).map_err(io)?;
            rows.map(|row| {
                let (recovery_id,revision,detached,forgotten,protected_active,protected_unknown,bytes)=row.map_err(io)?;
                Ok(RetentionMetadata {recovery_id,revision:u64::try_from(revision).map_err(io)?,
                    detached_since_ms:detached.map(|s|serde_json::from_str(&s).map_err(io)).transpose()?.flatten(),
                    forgotten,protected_active,protected_unknown,payload_bytes:u64::try_from(bytes).map_err(io)?})
            }).collect()
        }).await
    }
    async fn retire_detached(
        &self,
        id: &str,
        expected: u64,
        now: u64,
        reason: &str,
    ) -> Result<bool> {
        let id = id.to_owned();
        let reason = reason.to_owned();
        self.blocking(move |db| {
            let tx = db.transaction().map_err(io)?;
            let old: Option<String> = tx
                .query_row("SELECT record FROM sessions WHERE id=?1", [&id], |row| {
                    row.get(0)
                })
                .optional()
                .map_err(io)?;
            let old: SessionRecord =
                serde_json::from_str(&old.ok_or(StoreError::NotFound)?).map_err(io)?;
            let Some(new) =
                crate::retention::retirement_replacement(old.clone(), expected, now, &reason)?
            else {
                return Ok(false);
            };
            validate_replace(&old, &id, expected, &new)?;
            let changed = tx
                .execute(
                    "UPDATE sessions SET revision=?1,record=?2 WHERE id=?3 AND revision=?4",
                    params![
                        i64::try_from(new.revision).map_err(io)?,
                        serde_json::to_string(&new).map_err(io)?,
                        id,
                        i64::try_from(expected).map_err(io)?
                    ],
                )
                .map_err(io)?;
            if changed != 1 {
                return Err(StoreError::Conflict);
            }
            tx.commit().map_err(io)?;
            Ok(true)
        })
        .await
    }
    async fn list(&self) -> Result<Vec<SessionRecord>> {
        self.blocking(|db| {
            let mut statement = db
                .prepare("SELECT record FROM sessions ORDER BY id")
                .map_err(io)?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(io)?;
            rows.map(|row| serde_json::from_str(&row.map_err(io)?).map_err(io))
                .collect()
        })
        .await
    }
}
