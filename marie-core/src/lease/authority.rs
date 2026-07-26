use std::collections::HashMap;

use chrono::Utc;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};

use crate::{lease::protocol::{LeaseRequest, LeaseResponse, LeaseResult}, session::SessionId};

/// Machine à états du bail/epoch d'une session, répliquée via Raft (voir
/// `lease::storage::LeaseStateMachine`, qui l'enveloppe et l'appelle depuis
/// `RaftStateMachine::apply`) — cette logique elle-même est agnostique de
/// Raft : elle ne fait que muter ses deux `HashMap` en mémoire et renvoyer un
/// résultat, à charge de l'appelant de ne l'invoquer qu'après consensus.
#[derive(Default)]
pub struct LeaseAuthority {
    leases: HashMap<SessionId, LeaseEntry>,
    epochs: HashMap<SessionId, u64>,
}


impl LeaseAuthority {
    pub(crate) fn handle_request(&mut self, request: &LeaseRequest) -> LeaseResponse {
        use super::protocol::LeaseOp::{Acquire, Renew, Release};

        let result = match request.op {
            Acquire { holder, ttl } => self.acquire(request.session_id, holder, ttl),
            Renew { holder, epoch, ttl } => self.renew(request.session_id, holder, epoch, ttl),
            Release { holder, epoch } => self.release(request.session_id, holder, epoch),
        };

        LeaseResponse {
            request_id: request.request_id,
            result,
        }
    }

    fn release(&mut self, session_id: SessionId, holder: PeerId, epoch: u64) -> LeaseResult {
        use super::protocol::LeaseResult::{Renewed, Denied};

        if let Some(lease) = self.leases.get(&session_id) {
            if lease.holder == holder && lease.epoch == epoch {
                self.leases.remove(&session_id);
                return Renewed { expires_at: chrono::DateTime::<Utc>::default() };
            }
            return Denied { current_holder: lease.holder.clone(), current_epoch: lease.epoch };
        }
        Denied { current_holder: holder, current_epoch: 0 }
    }

    fn renew(&mut self, session_id: SessionId, holder: PeerId, epoch: u64, ttl: chrono::Duration) -> LeaseResult {
        use super::protocol::LeaseResult::{Renewed, Denied};
        let now = chrono::Utc::now();
        match self.leases.get_mut(&session_id) {
            Some(lease) if lease.holder == holder && lease.epoch == epoch => {
                lease.expires_at = now + ttl;
                Renewed { expires_at: lease.expires_at.clone() }
            }
            Some(lease) => Denied { current_holder: lease.holder.clone(), current_epoch: lease.epoch },
            None => LeaseResult::Denied { current_holder: holder, current_epoch: 0 },
        }
    }

    fn acquire(&mut self, session_id: SessionId, holder: PeerId, ttl: chrono::Duration) -> LeaseResult {
        use super::protocol::LeaseResult::{Granted, Denied};

        let now = chrono::Utc::now();
        if let Some(lease) = self.leases.get(&session_id)
            && !lease.has_expired(&now)
        {
            return Denied { current_holder: lease.holder.clone(), current_epoch: lease.epoch };
        }

        let epoch = self.epochs.entry(session_id).and_modify(|e| *e += 1).or_insert(1);
        let expires_at = now + ttl;
        let entry = LeaseEntry {
            holder,
            epoch: *epoch,
            expires_at: expires_at.clone()
        };
        self.leases.insert(session_id, entry);
        Granted { epoch: *epoch, expires_at }
    }
}

impl LeaseAuthority {
    /// Copie sérialisable de l'état courant — utilisée par
    /// `lease::storage::LeaseStateMachine` pour construire/installer un
    /// snapshot Raft (stockage en mémoire uniquement pour cette passe : voir
    /// la doc de `lease::storage`).
    pub(crate) fn snapshot(&self) -> LeaseSnapshot {
        LeaseSnapshot { leases: self.leases.clone(), epochs: self.epochs.clone() }
    }

    /// Remplace intégralement l'état courant par `snapshot` — appelé à
    /// l'installation d'un snapshot reçu du leader.
    pub(crate) fn restore(&mut self, snapshot: LeaseSnapshot) {
        self.leases = snapshot.leases;
        self.epochs = snapshot.epochs;
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct LeaseSnapshot {
    leases: HashMap<SessionId, LeaseEntry>,
    epochs: HashMap<SessionId, u64>,
}

#[derive(Clone, Serialize, Deserialize)]
struct LeaseEntry {
    holder: PeerId,
    epoch: u64,
    expires_at: chrono::DateTime<Utc>,
}

impl LeaseEntry {
    #[inline]
    pub fn has_expired(&self, t: &chrono::DateTime<Utc>) -> bool {
        self.expires_at <= *t
    }
}
