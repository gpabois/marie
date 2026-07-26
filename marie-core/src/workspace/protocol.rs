use serde::{Deserialize, Serialize};

use crate::{bail, err, events::EventEnvelope, session::SessionId, workspace::WorkspaceId};

/// Évènements de cycle de vie d'un workspace — même mécanique que
/// [`crate::session::SessionEvent`] (voir sa doc pour la justification du
/// schéma Layer/gossip) : chaque évènement est publié à la fois sur un topic
/// dédié au workspace, préfixé par son identifiant (voir [`Self::topic`] —
/// pour une passerelle qui relaie les évènements d'UN workspace à un client
/// WebSocket), et sur un topic global (voir [`Self::global_topic`] — pour un
/// abonné qui veut tout le cycle de vie sans connaître les identifiants à
/// l'avance). Seul [`server::WorkspaceServerActor`] en est l'émetteur :
/// chaque mutation réussie (voir [`server::WorkspaceCommand`]) produit
/// exactement l'évènement correspondant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkspaceEvent {
    Created { id: WorkspaceId },
    Removed { id: WorkspaceId },
    Replaced {id: WorkspaceId },
    SessionAdded { workspace_id: WorkspaceId, session_id: SessionId },
    SessionRemoved { workspace_id: WorkspaceId, session_id: SessionId },
    VarsPatched { workspace_id: WorkspaceId },
}


impl WorkspaceEvent {
    /// Racine commune à tous les topics de workspace, dédiés comme global —
    /// voir [`Self::is`].
    pub const TOPIC_PREFIX: &str = "marie/workspaces";

    /// Topic global, commun à tous les workspaces (voir [`Self::global_topic`])
    /// — conservé en plus du topic dédié (voir [`Self::topic_prefix`]) pour
    /// un abonné qui veut observer le cycle de vie de tous les workspaces
    /// sans connaître leurs identifiants à l'avance (ex. un tableau de bord).
    pub const GLOBAL_TOPIC_PREFIX: &str = "marie/workspaces/events";

    /// Workspace concerné par cet évènement — sert à calculer le topic dédié
    /// (voir [`Self::topic_prefix`]/[`Self::topic`]).
    pub fn workspace_id(&self) -> WorkspaceId {
        match self {
            WorkspaceEvent::Created { id } | WorkspaceEvent::Removed { id } => *id,
            WorkspaceEvent::SessionAdded { workspace_id, .. }
            | WorkspaceEvent::SessionRemoved { workspace_id, .. }
            | WorkspaceEvent::VarsPatched { workspace_id } => *workspace_id,
        }
    }

    /// Suffixe identifiant le type d'évènement, commun à [`Self::topic`] et
    /// [`Self::global_topic`].
    fn kind(&self) -> &'static str {
        match self {
            WorkspaceEvent::Created { .. } => "created",
            WorkspaceEvent::Removed { .. } => "removed",
            WorkspaceEvent::SessionAdded { .. } => "session-added",
            WorkspaceEvent::SessionRemoved { .. } => "session-removed",
            WorkspaceEvent::VarsPatched { .. } => "vars-patched",
        }
    }

    /// Topic dédié au workspace de cet évènement (`marie/workspaces/{id}/`,
    /// suffixé par le type d'évènement dans [`Self::topic`]) — un abonné
    /// n'ayant besoin que d'un workspace précis s'abonne uniquement à ce
    /// préfixe-ci plutôt qu'au flux de tous les workspaces.
    pub fn topic_prefix(&self) -> String {
        format!("{0}/{1}", Self::TOPIC_PREFIX, self.workspace_id())
    }

    /// Topic effectivement publié pour cet évènement, dédié à son workspace —
    /// voir [`Self::topic_prefix`]. Publié en plus de, et non à la place de,
    /// [`Self::global_topic`] (voir [`layers::WorkspaceEventLayer`]).
    pub fn topic(&self) -> String {
        format!("{0}/{1}", self.topic_prefix(), self.kind())
    }

    /// Topic global (sans l'identifiant de workspace), sous
    /// [`Self::GLOBAL_TOPIC_PREFIX`] — voir [`Self::topic`] pour le pendant
    /// dédié au workspace.
    pub fn global_topic(&self) -> String {
        format!("{0}/{1}", Self::GLOBAL_TOPIC_PREFIX, self.kind())
    }

    /// Reconnaît tout topic de workspace, dédié ou global — voir
    /// [`Self::topic_prefix`]/[`Self::GLOBAL_TOPIC_PREFIX`] pour filtrer plus
    /// précisément.
    pub fn is(msg: &EventEnvelope) -> bool {
        msg.topic.starts_with(Self::TOPIC_PREFIX)
    }

    /// Tous les suffixes de type d'évènement (voir [`Self::kind`]), dans le
    /// même ordre que les variantes de l'enum — même rôle que
    /// [`crate::session::SessionEvent::KINDS`], voir sa doc pour la limite
    /// (synchronisation manuelle avec [`Self::kind`]).
    pub const KINDS: [&'static str; 5] = [
        "created",
        "removed",
        "session-added",
        "session-removed",
        "vars-patched",
    ];

    /// Tous les topics globaux (un par type d'évènement, voir
    /// [`Self::KINDS`]/[`Self::global_topic`]).
    pub fn all_global_topics() -> Vec<String> {
        Self::KINDS.iter().map(|kind| format!("{}/{kind}", Self::GLOBAL_TOPIC_PREFIX)).collect()
    }
}

impl TryFrom<EventEnvelope> for WorkspaceEvent {
    type Error = crate::Error;

    fn try_from(value: EventEnvelope) -> Result<Self, Self::Error> {

        if !Self::is(&value) { bail!("the pubsub message payload is not a workspace event") };

        serde_json::from_slice(&value.payload).map_err(|_| err!("the pubsub message payload is not a workspace event"))
    }
}