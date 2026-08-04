use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{hitl::HitlId, session::{SessionId, channel::Channels, run_log::RunLog, snapshot::SnapshotRef}};
use super::{FramePolicy, FrameStatus, FrameId, FrameSpecRef, FrameKind};

#[derive(TypedBuilder)]
pub struct NewFrameNodeArgs {
    session_id: SessionId,
    spec_ref: FrameSpecRef,
    data: FrameKind,
    #[builder(default)]
    inherited_channels: Channels,
    #[builder(default, setter(strip_option))]
    forked_from: Option<SnapshotRef>,
    #[builder(default)]
    frame_policy: FramePolicy,
    #[builder(default)]
    barrier: bool,
    #[builder(default)]
    superstep: u32
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameNode {
    pub session_id: SessionId,
    pub id: FrameId,
    pub status: FrameStatus,
    pub frame_policy: FramePolicy,
    pub superstep: u32,
    pub barrier: bool,
    pub forked_from: Option<SnapshotRef>,
    pub inherited_channels: Channels,
    pub spec_ref: FrameSpecRef,
    pub data: FrameKind,
    #[serde(default)]
    pub logs: Vec<RunLog>,
    /// Alias reliant l'index d'une entrée de [`Self::logs`] à l'[`HitlId`]
    /// de la requête human-in-the-loop qu'elle attend — même principe que
    /// `vfs::alias::PostgresAliasCatalog` (une correspondance consultable
    /// dans les deux sens), voir [`Self::bind_hitl_to_log`]/
    /// [`Self::hitl_id_of_log`]/[`Self::log_index_of_hitl`]. Distinct de
    /// [`FrameData::Hitl`], qui lie plutôt le frame *enfant* créé pour la
    /// requête à ce même `HitlId` : ici c'est l'entrée du journal de rejeu
    /// du frame *appelant* (celui qui a réservé le [`RunLogContent::HitlLog`](
    /// crate::session::run_log::RunLogContent::HitlLog) puis attend sa
    /// résolution) qui est visée.
    #[serde(default)]
    pub(super) hitl_aliases: HashMap<u32, HitlId>,
    /// Racine de l'arbre de sa session — au plus un [`FrameNode`] par
    /// `session_id` peut porter `is_root = true` à la fois : voir
    /// `StoreSessionFrame::upsert_frame`, qui rabaisse les autres avant
    /// d'écrire celui-ci.
    #[serde(default)]
    pub is_root: bool,
    pub(super) parent: Option<FrameId>,
    pub(super) next_sibling: Option<FrameId>,
    pub(super) prev_sibling: Option<FrameId>,
    pub(super) children: Vec<FrameId>,
}

impl FrameNode {
    pub fn is_waiting_children(&self) -> bool {
        matches!(self.status, FrameStatus::WaitingChildren)
    }

    pub fn has_terminated(&self) -> bool {
        matches!(self.status, FrameStatus::Failed(_) | FrameStatus::Completed)
    }

    pub fn iter_children(&self) -> impl Iterator<Item=FrameId> {
        self.children.iter().cloned()
    }

    /// Lie l'entrée `index` de [`Self::logs`] à `hitl_id` — écrase un alias
    /// déjà posé sur cet `index` s'il y en avait un, comme
    /// `StoreSessionFrame::bind_hitl_to_frame` pour le lien frame/hitl.
    pub fn bind_hitl_to_log(&mut self, index: u32, hitl_id: HitlId) {
        self.hitl_aliases.insert(index, hitl_id);
    }

    /// L'[`HitlId`] lié à l'entrée `index` de [`Self::logs`], s'il y en a
    /// un — voir [`Self::bind_hitl_to_log`].
    pub fn hitl_id_of_log(&self, index: u32) -> Option<HitlId> {
        self.hitl_aliases.get(&index).copied()
    }

    /// L'index de [`Self::logs`] lié à `hitl_id`, s'il y en a un — sens
    /// inverse de [`Self::hitl_id_of_log`]. Linéaire en le nombre d'alias
    /// posés sur ce frame, toujours petit (voir la doc de
    /// [`crate::session::run_log::RunLogs`] : au plus une réservation non
    /// résolue à la fois).
    pub fn log_index_of_hitl(&self, hitl_id: HitlId) -> Option<u32> {
        self.hitl_aliases.iter().find_map(|(index, id)| (*id == hitl_id).then_some(*index))
    }
}

impl FrameNode {
    pub fn new(id: FrameId, args: NewFrameNodeArgs) -> Self 
    {
        Self {
            session_id: args.session_id,
            id,
            data: args.data,
            forked_from: args.forked_from,
            status: FrameStatus::default(),
            inherited_channels: args.inherited_channels,
            superstep: args.superstep,
            frame_policy: args.frame_policy,
            barrier: args.barrier,
            spec_ref: args.spec_ref,
            logs: vec![],
            hitl_aliases: HashMap::new(),
            is_root: false,
            parent: None,
            next_sibling: None,
            prev_sibling: None,
            children: vec![],
        }
    }
}

