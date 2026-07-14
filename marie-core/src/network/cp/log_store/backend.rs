use std::ops::Bound;

use openraft::{Entry, LogId, Vote};

use crate::network::cp::types::{RaftNodeId, TypeConfig};

/// Backend de stockage durable du log Raft (write-ahead log) et du vote
/// courant — voir [`super::LogStore`], qui adapte ce trait aux traits
/// `RaftLogReader`/`RaftLogStorage` attendus par openraft.
///
/// Abstrait du moteur choisi (redb par défaut, voir
/// [`super::redb_backend::RedbLogBackend`]) pour laisser l'utilisateur de la
/// librairie brancher ce qu'il préfère (Postgres, sled, etc.) sans toucher
/// au reste du control plane — même principe que les traits CRUD spécifiques
/// des catalogues (`model::catalog::store::ModelStore`,
/// `tools::catalog::store::ToolStore`, ...), séparé d'eux parce que le log a
/// un patron d'accès différent (scans ordonnés par index, voir
/// [`Self::entries`]) qu'une table `get`/`put`/`list` générique ne sert pas
/// efficacement.
///
/// Toute méthode qui modifie l'état (`append`/`truncate`/`purge`/`save_vote`)
/// *doit* avoir rendu ses effets durables (fsync ou équivalent) avant de
/// retourner `Ok` : [`super::LogStore::append`] n'appelle le callback
/// `LogFlushed` d'openraft qu'une fois [`Self::append`] résolue, et openraft
/// ne considère une entrée "commit" qu'une fois qu'une majorité l'a
/// *réellement* persistée — pas seulement acceptée en mémoire.
#[async_trait::async_trait]
pub trait RaftLogBackend: Send + Sync {
    /// `(dernier log purgé, dernier log connu)`, tels que persistés — voir
    /// `RaftLogStorage::get_log_state`. Le second doit tenir compte du
    /// premier si le log est vide (tout purgé jusque-là).
    async fn log_state(&self) -> anyhow::Result<(Option<LogId<RaftNodeId>>, Option<LogId<RaftNodeId>>)>;

    /// Entrées dont l'index (voir `LogId::index`) appartient à `range`,
    /// triées par index croissant.
    async fn entries(&self, range: (Bound<u64>, Bound<u64>)) -> anyhow::Result<Vec<Entry<TypeConfig>>>;

    /// Ajoute `entries` — voir la note du trait sur la durabilité.
    async fn append(&self, entries: Vec<Entry<TypeConfig>>) -> anyhow::Result<()>;

    /// Retire toute entrée d'index >= `index` (log divergent après une
    /// élection, voir `RaftLogStorage::truncate`).
    async fn truncate(&self, index: u64) -> anyhow::Result<()>;

    /// Retire toute entrée d'index <= `log_id.index` et enregistre
    /// durablement `log_id` comme dernier purgé (compaction après snapshot,
    /// voir `RaftLogStorage::purge`).
    async fn purge(&self, log_id: LogId<RaftNodeId>) -> anyhow::Result<()>;

    async fn save_vote(&self, vote: Vote<RaftNodeId>) -> anyhow::Result<()>;
    async fn read_vote(&self) -> anyhow::Result<Option<Vote<RaftNodeId>>>;
}
