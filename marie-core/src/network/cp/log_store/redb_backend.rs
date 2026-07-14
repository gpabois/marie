use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use openraft::{Entry, LogId, Vote};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::backend::RaftLogBackend;
use crate::network::cp::types::{RaftNodeId, TypeConfig};

const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

const VOTE_KEY: &str = "vote";
const LAST_PURGED_KEY: &str = "last_purged";

/// Implémentation de [`RaftLogBackend`] adossée à [`redb`] — un fichier
/// dédié, distinct de celui des catalogues (voir
/// `persistency::store::RedbStore`) : le log Raft a un patron d'accès
/// différent (scans ordonnés par index, voir [`RaftLogBackend::entries`])
/// que la table générique `kv` de `RedbStore` ne sert pas efficacement.
pub struct RedbLogBackend {
    db: Arc<redb::Database>,
}

impl RedbLogBackend {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = redb::Database::create(path)?;

        // Crée les tables dès l'ouverture : évite d'avoir à distinguer
        // "table absente" de "clé absente" dans le reste des méthodes (même
        // choix que `RedbStore::open`).
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(LOG_TABLE)?;
            let _ = write_txn.open_table(META_TABLE)?;
        }
        write_txn.commit()?;

        Ok(Self { db: Arc::new(db) })
    }
}

#[async_trait::async_trait]
impl RaftLogBackend for RedbLogBackend {
    async fn log_state(&self) -> anyhow::Result<(Option<LogId<RaftNodeId>>, Option<LogId<RaftNodeId>>)> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || -> anyhow::Result<(Option<LogId<RaftNodeId>>, Option<LogId<RaftNodeId>>)> {
            let read_txn = db.begin_read()?;

            let last_purged: Option<LogId<RaftNodeId>> = {
                let table = read_txn.open_table(META_TABLE)?;
                table.get(LAST_PURGED_KEY)?.map(|value| decode(value.value())).transpose()?
            };

            let last_in_log: Option<LogId<RaftNodeId>> = {
                let table = read_txn.open_table(LOG_TABLE)?;
                table.last()?.map(|(_, value)| decode::<Entry<TypeConfig>>(value.value())).transpose()?.map(|entry| entry.log_id)
            };

            // Le dernier log connu est celui du log lui-même s'il n'est pas
            // vide, sinon le dernier purgé (tout le log tient dans un
            // snapshot déjà installé) — voir `RaftLogStorage::get_log_state`.
            Ok((last_purged, last_in_log.or(last_purged)))
        })
        .await?
    }

    async fn entries(&self, range: (Bound<u64>, Bound<u64>)) -> anyhow::Result<Vec<Entry<TypeConfig>>> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<Entry<TypeConfig>>> {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(LOG_TABLE)?;

            let mut entries = Vec::new();
            for result in table.range(range)? {
                let (_, value) = result?;
                entries.push(decode(value.value())?);
            }
            Ok(entries)
        })
        .await?
    }

    async fn append(&self, entries: Vec<Entry<TypeConfig>>) -> anyhow::Result<()> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(LOG_TABLE)?;
                for entry in &entries {
                    table.insert(entry.log_id.index, encode(entry)?.as_slice())?;
                }
            }
            // Durable dès ce `commit()` (redb fsync par défaut) : c'est ce
            // qui autorise `LogStore::append` à signaler la réussite à
            // openraft (voir la note sur `RaftLogBackend`).
            write_txn.commit()?;
            Ok(())
        })
        .await?
    }

    async fn truncate(&self, index: u64) -> anyhow::Result<()> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(LOG_TABLE)?;
                table.retain(|key, _| key < index)?;
            }
            write_txn.commit()?;
            Ok(())
        })
        .await?
    }

    async fn purge(&self, log_id: LogId<RaftNodeId>) -> anyhow::Result<()> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(LOG_TABLE)?;
                table.retain(|key, _| key > log_id.index)?;

                let mut meta = write_txn.open_table(META_TABLE)?;
                meta.insert(LAST_PURGED_KEY, encode(&log_id)?.as_slice())?;
            }
            write_txn.commit()?;
            Ok(())
        })
        .await?
    }

    async fn save_vote(&self, vote: Vote<RaftNodeId>) -> anyhow::Result<()> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(META_TABLE)?;
                table.insert(VOTE_KEY, encode(&vote)?.as_slice())?;
            }
            write_txn.commit()?;
            Ok(())
        })
        .await?
    }

    async fn read_vote(&self) -> anyhow::Result<Option<Vote<RaftNodeId>>> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || -> anyhow::Result<Option<Vote<RaftNodeId>>> {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(META_TABLE)?;
            table.get(VOTE_KEY)?.map(|value| decode(value.value())).transpose()
        })
        .await?
    }
}

fn encode<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    // Uniquement des types Raft internes : la sérialisation JSON ne peut pas
    // échouer en pratique (même choix que `persistency::store::RedbStore`).
    Ok(serde_json::to_vec(value)?)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> anyhow::Result<T> {
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
mod tests {
    use openraft::{EntryPayload, LeaderId, Vote};

    use super::*;
    use crate::network::cp::types::ControlPlaneRequest;

    struct TempPath(std::path::PathBuf);

    impl Drop for TempPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn temp_backend() -> (RedbLogBackend, TempPath) {
        let path = TempPath(std::env::temp_dir().join(format!("marie-raft-log-test-{}.redb", crate::id::generate_id())));
        let backend = RedbLogBackend::open(&path.0).unwrap();
        (backend, path)
    }

    fn entry(index: u64) -> Entry<TypeConfig> {
        Entry { log_id: LogId::new(LeaderId::new(1, 1), index), payload: EntryPayload::Blank }
    }

    #[tokio::test]
    async fn test_append_then_read_entries_in_range() {
        let (backend, _path) = temp_backend();

        backend.append(vec![entry(1), entry(2), entry(3)]).await.unwrap();

        let entries = backend.entries((Bound::Included(2), Bound::Unbounded)).await.unwrap();
        assert_eq!(entries.iter().map(|e| e.log_id.index).collect::<Vec<_>>(), vec![2, 3]);
    }

    #[tokio::test]
    async fn test_log_state_reflects_last_entry() {
        let (backend, _path) = temp_backend();

        assert_eq!(backend.log_state().await.unwrap(), (None, None));

        backend.append(vec![entry(1), entry(2)]).await.unwrap();

        let (last_purged, last_log) = backend.log_state().await.unwrap();
        assert_eq!(last_purged, None);
        assert_eq!(last_log.map(|id| id.index), Some(2));
    }

    #[tokio::test]
    async fn test_truncate_removes_entries_from_index() {
        let (backend, _path) = temp_backend();
        backend.append(vec![entry(1), entry(2), entry(3)]).await.unwrap();

        backend.truncate(2).await.unwrap();

        let entries = backend.entries((Bound::Unbounded, Bound::Unbounded)).await.unwrap();
        assert_eq!(entries.iter().map(|e| e.log_id.index).collect::<Vec<_>>(), vec![1]);
    }

    #[tokio::test]
    async fn test_purge_removes_entries_up_to_index_and_records_last_purged() {
        let (backend, _path) = temp_backend();
        backend.append(vec![entry(1), entry(2), entry(3)]).await.unwrap();

        let purge_point = LogId::new(LeaderId::new(1, 1), 2);
        backend.purge(purge_point).await.unwrap();

        let entries = backend.entries((Bound::Unbounded, Bound::Unbounded)).await.unwrap();
        assert_eq!(entries.iter().map(|e| e.log_id.index).collect::<Vec<_>>(), vec![3]);

        let (last_purged, last_log) = backend.log_state().await.unwrap();
        assert_eq!(last_purged.map(|id| id.index), Some(2));
        assert_eq!(last_log.map(|id| id.index), Some(3));
    }

    #[tokio::test]
    async fn test_save_then_read_vote_round_trip() {
        let (backend, _path) = temp_backend();

        assert_eq!(backend.read_vote().await.unwrap(), None);

        let vote = Vote::new(3, 42u64);
        backend.save_vote(vote).await.unwrap();

        assert_eq!(backend.read_vote().await.unwrap(), Some(vote));
    }

    #[tokio::test]
    async fn test_entries_survive_reopening_the_same_file() {
        let (backend, path) = temp_backend();
        backend.append(vec![entry(1)]).await.unwrap();
        drop(backend);

        let reopened = RedbLogBackend::open(&path.0).unwrap();
        let entries = reopened.entries((Bound::Unbounded, Bound::Unbounded)).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id.index, 1);
    }

    #[test]
    fn test_entry_payload_round_trips_through_json() {
        // Vérifie que `ControlPlaneRequest` (le `D` de `TypeConfig`) survit
        // bien à l'encodage JSON utilisé par ce backend — pas seulement
        // `EntryPayload::Blank`, testé ailleurs.
        let entry = Entry {
            log_id: LogId::new(LeaderId::new(1, 1), 1),
            payload: EntryPayload::<TypeConfig>::Normal(ControlPlaneRequest::RegisterPersistency { peer_id: libp2p::PeerId::random() }),
        };

        let encoded = encode(&entry).unwrap();
        let decoded: Entry<TypeConfig> = decode(&encoded).unwrap();
        assert_eq!(decoded.log_id, entry.log_id);
    }
}
