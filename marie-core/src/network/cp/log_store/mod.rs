//! Stockage du log Raft (write-ahead log du cluster) + du vote courant.
//!
//! [`LogStore`] adapte un [`RaftLogBackend`] — durable, technologie au choix
//! de l'utilisateur de la librairie (redb par défaut, voir
//! [`redb_backend::RedbLogBackend`]) — aux traits `RaftLogReader`/
//! `RaftLogStorage` attendus par openraft. Sans ce backend durable, un
//! redémarrage complet du cluster perd tout `ControlPlaneState` (jobs,
//! registre des workers, etc.) : rendre le log durable suffit à le
//! reconstruire intégralement, openraft rejouant les entrées persistées à
//! travers `RaftStateMachine::apply` au démarrage (voir
//! `state::ControlPlaneStateMachineStore`, qui n'a pas besoin de son propre
//! mécanisme de snapshot durable pour ça — seulement d'un log complet).

pub mod backend;
pub mod redb_backend;

use std::ops::Bound;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{Entry, LogId, OptionalSend, StorageError, StorageIOError, Vote};

pub use backend::RaftLogBackend;

use super::types::{RaftNodeId, TypeConfig};

/// Traduit une borne empruntée (`RangeBounds::start_bound`/`end_bound`,
/// `Bound<&u64>`) en borne possédée (`Bound<u64>`) — [`RaftLogBackend`] est
/// utilisé via `dyn`, donc ne peut pas rester générique sur le type de range
/// comme le `try_get_log_entries` d'openraft l'est.
fn owned_bound(bound: Bound<&u64>) -> Bound<u64> {
    match bound {
        Bound::Included(index) => Bound::Included(*index),
        Bound::Excluded(index) => Bound::Excluded(*index),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// `anyhow::Error` n'implémente pas `std::error::Error` (choix de la crate),
/// donc pas non plus `Into<AnyError>` sans la feature `anyhow` de la crate
/// `anyerror` (transitive via openraft, non activée ici) — un
/// `std::io::Error` fait l'affaire à la place, lui l'implémente.
fn io_error(error: anyhow::Error) -> std::io::Error {
    std::io::Error::other(error.to_string())
}

#[derive(Clone)]
pub struct LogStore {
    backend: Arc<dyn RaftLogBackend>,
}

impl LogStore {
    pub fn new(backend: Arc<dyn RaftLogBackend>) -> Self {
        Self { backend }
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<RaftNodeId>> {
        let bounds = (owned_bound(range.start_bound()), owned_bound(range.end_bound()));
        self.backend.entries(bounds).await.map_err(|error| StorageIOError::read_logs(&io_error(error)).into())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<RaftNodeId>> {
        let (last_purged_log_id, last_log_id) =
            self.backend.log_state().await.map_err(|error| StorageIOError::read_logs(&io_error(error)))?;
        Ok(LogState { last_purged_log_id, last_log_id })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
        self.backend.save_vote(*vote).await.map_err(|error| StorageIOError::write_vote(&io_error(error)).into())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<RaftNodeId>>, StorageError<RaftNodeId>> {
        self.backend.read_vote().await.map_err(|error| StorageIOError::read_vote(&io_error(error)).into())
    }

    /// Ajoute des entrées au log. `callback` DOIT être appelé une fois les
    /// entrées durablement persistées — c'est ce qui débloque la
    /// réplication côté openraft (voir `LogFlushed`, et la note sur
    /// [`RaftLogBackend`]).
    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> Result<(), StorageError<RaftNodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();

        match self.backend.append(entries).await {
            Ok(()) => callback.log_io_completed(Ok(())),
            Err(error) => callback.log_io_completed(Err(std::io::Error::other(error.to_string()))),
        }

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
        self.backend.truncate(log_id.index).await.map_err(|error| StorageIOError::write_logs(&io_error(error)).into())
    }

    async fn purge(&mut self, log_id: LogId<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
        self.backend.purge(log_id).await.map_err(|error| StorageIOError::write_logs(&io_error(error)).into())
    }
}
