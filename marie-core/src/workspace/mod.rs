pub mod client;
#[cfg(feature = "catalog")]
pub mod layers;
pub mod model;
pub mod rpc;
// `server::WorkspaceCommand` est référencé directement par les RPC mutantes
// de `rpc.rs` (voir ex. `InsertWorkspace`), lui-même requis par
// `client::WorkspaceClient` — impossible de gater derrière `catalog`, voir
// la même remarque sur `crate::session::server`.
pub mod server;
pub mod store;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::pubsub::PubSubMessage;
use crate::session::SessionId;

pub use model::{Workspace, WorkspaceId};
pub use rpc::{AddSession, GetWorkspace, InsertWorkspace, ListWorkspace, PatchVars, QueryVars, RemoveSession, RemoveWorkspace};

pub const NS_WORKSPACE: &str = "/marie/ns/workspaces";

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
    SessionAdded { workspace_id: WorkspaceId, session_id: SessionId },
    SessionRemoved { workspace_id: WorkspaceId, session_id: SessionId },
    VarsPatched { workspace_id: WorkspaceId },
}

#[derive(Debug, Error)]
pub enum WorkspaceEventError {
    #[error("ce n'est pas un évènement de workspace")]
    NotWorkspaceEvent,
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
    pub fn is(msg: &PubSubMessage) -> bool {
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

impl TryFrom<PubSubMessage> for WorkspaceEvent {
    type Error = WorkspaceEventError;

    fn try_from(value: PubSubMessage) -> Result<Self, Self::Error> {
        use WorkspaceEventError::NotWorkspaceEvent;

        if !Self::is(&value) { return Err(NotWorkspaceEvent) };

        serde_json::from_slice(&value.payload).map_err(|_| NotWorkspaceEvent)
    }
}

/// Construit un [`server::WorkspaceServer`] en chaînant le transport réseau
/// brut (`NetworkCommand`/`NetworkEvent`) à travers `PubSubLayer` puis
/// [`layers::WorkspaceEventLayer`] — miroir de
/// [`crate::session::build_server`].
#[cfg(feature = "catalog")]
pub fn build_server(net: &crate::network::actor::Network, args: server::WorkspaceServerArgs) -> server::WorkspaceServer {
    use crate::layer::{IntoService as _, LayerExt as _};
    use crate::pubsub::layers::PubSubLayer;

    net.transport()
        .chain::<PubSubLayer, _>(())
        .chain::<layers::WorkspaceEventLayer, _>(())
        .into_service(args)
}

/// Charge utile de [`rpc::AddSession`]/[`rpc::RemoveSession`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceSessionRequest {
    pub workspace_id: WorkspaceId,
    pub session_id: SessionId,
}

/// Charge utile de [`rpc::QueryVars`] : `path` est une expression JSONPath
/// (voir la crate `jsonpath_lib`), évaluée contre [`Workspace::vars`] traité
/// comme un unique document JSON (ses clés de premier niveau devenant les
/// champs de ce document, ex: `$.budget`) — même sémantique que
/// [`crate::session::SessionVarsQueryRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceVarsQueryRequest {
    pub workspace_id: WorkspaceId,
    pub path: String,
}

/// Charge utile de [`rpc::PatchVars`] : remplace, dans [`Workspace::vars`]
/// traité comme un document JSON unique (voir [`WorkspaceVarsQueryRequest`]),
/// chaque nœud correspondant à `path` par `value`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceVarsPatchRequest {
    pub workspace_id: WorkspaceId,
    pub path: String,
    pub value: Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::generate_id;

    #[test]
    fn topics_are_prefixed_by_workspace_id() {
        let id = WorkspaceId::new(generate_id());
        let event = WorkspaceEvent::Created { id };

        assert_eq!(event.topic(), format!("marie/workspaces/{id}/created"));
        assert_eq!(event.global_topic(), "marie/workspaces/events/created");
    }

    #[test]
    fn event_roundtrips_through_pubsub_message() {
        let id = WorkspaceId::new(generate_id());
        let session_id = SessionId::new(generate_id());
        let event = WorkspaceEvent::SessionAdded { workspace_id: id, session_id };

        let msg = PubSubMessage {
            id: String::default(),
            source: None,
            topic: event.topic(),
            payload: serde_json::to_vec(&event).unwrap(),
        };

        let decoded = WorkspaceEvent::try_from(msg).unwrap();
        assert!(matches!(
            decoded,
            WorkspaceEvent::SessionAdded { workspace_id, session_id: s } if workspace_id == id && s == session_id
        ));
    }

    #[test]
    fn foreign_topic_is_rejected() {
        let msg = PubSubMessage {
            id: String::default(),
            source: None,
            topic: "marie/sessions/whatever".to_string(),
            payload: Vec::new(),
        };

        assert!(WorkspaceEvent::try_from(msg).is_err());
    }

    #[test]
    fn all_global_topics_has_one_per_kind() {
        let topics = WorkspaceEvent::all_global_topics();
        assert_eq!(topics.len(), WorkspaceEvent::KINDS.len());
        assert!(topics.iter().all(|t| t.starts_with(WorkspaceEvent::GLOBAL_TOPIC_PREFIX)));
    }
}
