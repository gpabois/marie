//! Stockage Raft en mémoire pour [`super::raft::LeaseTypeConfig`] — pas de
//! persistance disque dans cette passe : les baux sont bornés par TTL, donc
//! perdre l'état du groupe Raft à un redémarrage complet du cluster est
//! acceptable (un nœud qui redémarre seul rejoint simplement avec un journal
//! vide et se resynchronise depuis les autres via la réplication Raft
//! normale). Une implémentation adossée à `persistency::RedbStore`, suivant
//! le patron `*Store for RedbStore` déjà utilisé ailleurs
//! (`persistency::session::SessionStore`), est un candidat naturel pour une
//! passe future si la durabilité locale devient nécessaire.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use chrono::Utc;
use openraft::{
    storage::{LogFlushed, RaftLogStorage, RaftStateMachine},
    LogId, LogState, OptionalSend, RaftLogReader, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError,
    StoredMembership, Vote,
};
use parking_lot::Mutex;

use crate::lease::{
    authority::{LeaseAuthority, LeaseSnapshot},
    protocol::LeaseResult,
    raft::{LeaseNodeId, LeaseTypeConfig},
};

type Entry = openraft::Entry<LeaseTypeConfig>;
type LeaseNode = crate::node::NodeId;

/// Journal Raft (votes + entrées) en mémoire, protégé par un simple
/// `Mutex` — pas de contention notable attendue : un seul `RaftCore` par
/// nœud possède ce journal, `get_log_reader` ne fait que cloner ce handle
/// pour les tâches de réplication.
#[derive(Clone, Default)]
pub struct LeaseLogStore(Arc<Mutex<LogStoreInner>>);

#[derive(Default)]
struct LogStoreInner {
    log: BTreeMap<u64, Entry>,
    vote: Option<Vote<LeaseNodeId>>,
    last_purged_log_id: Option<LogId<LeaseNodeId>>,
}

// Pas de `#[async_trait]` sur ces `impl` : les traits `storage::v2` d'openraft
// utilisent leur propre macro (`#[add_async_trait]`, support natif des
// `async fn` en trait), incompatible avec la désucrification du crate
// `async-trait` (durées de vie différentes de la déclaration — E0195).
impl RaftLogReader<LeaseTypeConfig> for LeaseLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError<LeaseNodeId>> {
        Ok(self.0.lock().log.range(range).map(|(_, entry)| entry.clone()).collect())
    }
}

impl RaftLogStorage<LeaseTypeConfig> for LeaseLogStore {
    type LogReader = LeaseLogStore;

    async fn get_log_state(&mut self) -> Result<LogState<LeaseTypeConfig>, StorageError<LeaseNodeId>> {
        let inner = self.0.lock();
        let last_log_id = inner.log.values().last().map(|entry| entry.log_id).or(inner.last_purged_log_id);
        Ok(LogState { last_purged_log_id: inner.last_purged_log_id, last_log_id })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<LeaseNodeId>) -> Result<(), StorageError<LeaseNodeId>> {
        self.0.lock().vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<LeaseNodeId>>, StorageError<LeaseNodeId>> {
        Ok(self.0.lock().vote.clone())
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<LeaseTypeConfig>) -> Result<(), StorageError<LeaseNodeId>>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut inner = self.0.lock();
            for entry in entries {
                inner.log.insert(entry.log_id.index, entry);
            }
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<LeaseNodeId>) -> Result<(), StorageError<LeaseNodeId>> {
        // Retire `log_id` (inclus) et tout ce qui suit : `split_off` renvoie
        // la queue >= la clé et ne garde que la tête < la clé dans `log`.
        self.0.lock().log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<LeaseNodeId>) -> Result<(), StorageError<LeaseNodeId>> {
        let mut inner = self.0.lock();
        inner.log.retain(|&index, _| index > log_id.index);
        inner.last_purged_log_id = Some(log_id);
        Ok(())
    }
}

/// Machine à états Raft : enveloppe [`LeaseAuthority`] (logique métier
/// inchangée) et le suivi Raft (dernier log appliqué, dernière composition
/// de groupe, snapshot courant).
#[derive(Clone, Default)]
pub struct LeaseStateMachine(Arc<Mutex<StateMachineInner>>);

#[derive(Default)]
struct StateMachineInner {
    authority: LeaseAuthority,
    last_applied: Option<LogId<LeaseNodeId>>,
    last_membership: StoredMembership<LeaseNodeId, LeaseNode>,
    current_snapshot: Option<StoredSnapshot>,
}

#[derive(Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<LeaseNodeId, LeaseNode>,
    data: Vec<u8>,
}

impl RaftStateMachine<LeaseTypeConfig> for LeaseStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<LeaseNodeId>>, StoredMembership<LeaseNodeId, LeaseNode>), StorageError<LeaseNodeId>> {
        let inner = self.0.lock();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<LeaseResult>, StorageError<LeaseNodeId>>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        use openraft::EntryPayload::{Blank, Membership, Normal};

        let mut inner = self.0.lock();
        let mut responses = Vec::new();

        for entry in entries {
            inner.last_applied = Some(entry.log_id);

            match entry.payload {
                // Entrée vide (heartbeat de prise de leadership) ou de
                // changement de composition : aucune réponse applicative
                // n'est attendue par un appelant `client_write` (ces entrées
                // ne proviennent jamais de `LeaseClient`) — la valeur ci-
                // dessous n'est donc jamais observée, voir la doc de
                // `RaftStateMachine::apply`.
                Blank => responses.push(LeaseResult::NotLeader { leader_hint: None }),
                Membership(membership) => {
                    inner.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    responses.push(LeaseResult::NotLeader { leader_hint: None });
                }
                Normal(request) => {
                    let response = inner.authority.handle_request(&request);
                    responses.push(response.result);
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<LeaseNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<LeaseNodeId, LeaseNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<LeaseNodeId>> {
        let data = snapshot.into_inner();

        let restored: LeaseSnapshot = serde_json::from_slice(&data).unwrap_or_default();

        let mut inner = self.0.lock();
        inner.authority.restore(restored);
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.current_snapshot = Some(StoredSnapshot { meta: meta.clone(), data });

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<LeaseTypeConfig>>, StorageError<LeaseNodeId>> {
        Ok(self.0.lock().current_snapshot.clone().map(|stored| Snapshot {
            meta: stored.meta,
            snapshot: Box::new(Cursor::new(stored.data)),
        }))
    }
}

impl RaftSnapshotBuilder<LeaseTypeConfig> for LeaseStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<LeaseTypeConfig>, StorageError<LeaseNodeId>> {
        let mut inner = self.0.lock();

        let data = serde_json::to_vec(&inner.authority.snapshot()).unwrap_or_default();
        let snapshot_id = format!("{}-{}", inner.last_applied.map(|id| id.index).unwrap_or(0), Utc::now().timestamp_nanos_opt().unwrap_or_default());

        let meta = SnapshotMeta {
            last_log_id: inner.last_applied,
            last_membership: inner.last_membership.clone(),
            snapshot_id,
        };

        inner.current_snapshot = Some(StoredSnapshot { meta: meta.clone(), data: data.clone() });

        Ok(Snapshot { meta, snapshot: Box::new(Cursor::new(data)) })
    }
}
