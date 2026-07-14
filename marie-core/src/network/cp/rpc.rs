use libp2p::request_response;
use serde::{Deserialize, Serialize};

use crate::{
    expert::{catalog::ExpertId, declaration::ExpertDeclaration},
    job::{Job, JobId, JobState},
    mode::state_graph::{catalog::StateGraphId, declaration::StateGraphDeclaration},
    model::declaration::{Model, ModelId},
    session::SessionId,
    tools::{catalog::ToolId, declaration::ToolDeclaration},
    workspace::WorkspaceId,
};

/// Represents a Rpc Call
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcCall {
    pub name: String,
    pub args: serde_json::Value
}

impl RpcCall {
    pub const GET_MODEL: &str = "get-model";
    /// Client -> control plane : crée ou remplace la déclaration d'un modèle
    /// (répliqué via Raft, voir `ControlPlaneRequest::SetModel`). Les
    /// arguments sont un [`SetModelRequest`].
    pub const SET_MODEL: &str = "set-model";
    /// Client -> control plane : retire un modèle du catalogue (répliqué via
    /// Raft, voir `ControlPlaneRequest::RemoveModel`). Les arguments sont un
    /// [`ModelId`].
    pub const REMOVE_MODEL: &str = "remove-model";
    /// Client -> control plane : liste tout le catalogue. Comme
    /// `GET_MODEL`, chaque clé API est chiffrée spécifiquement pour le nœud
    /// appelant (voir `SecretManager::encrypt_api_key`) — jamais en clair.
    pub const LIST_MODELS: &str = "list-models";
    /// Client -> control plane : crée ou remplace la déclaration d'un tool
    /// (répliqué via Raft, voir `ControlPlaneRequest::SetTool`). Les
    /// arguments sont un [`SetToolRequest`]. Ne dit rien de qui exécute ce
    /// tool — voir `RpcCall::REGISTER_RPC` et
    /// `tools::client::ToolClient::register_executor`.
    pub const SET_TOOL: &str = "set-tool";
    /// Client -> control plane : retire un tool du catalogue (répliqué via
    /// Raft, voir `ControlPlaneRequest::RemoveTool`). Les arguments sont un
    /// [`ToolId`].
    pub const REMOVE_TOOL: &str = "remove-tool";
    /// Client -> control plane : récupère la déclaration d'un tool. Les
    /// arguments sont un [`ToolId`].
    pub const GET_TOOL: &str = "get-tool";
    /// Client -> control plane : liste tout le catalogue de tools.
    pub const LIST_TOOLS: &str = "list-tools";
    /// Client -> control plane : crée ou remplace la déclaration d'un expert
    /// (répliqué via Raft, voir `ControlPlaneRequest::SetExpert`). Les
    /// arguments sont un [`SetExpertRequest`].
    pub const SET_EXPERT: &str = "set-expert";
    /// Client -> control plane : retire un expert du catalogue (répliqué via
    /// Raft, voir `ControlPlaneRequest::RemoveExpert`). Les arguments sont un
    /// [`ExpertId`].
    pub const REMOVE_EXPERT: &str = "remove-expert";
    /// Client -> control plane : récupère la déclaration d'un expert. Les
    /// arguments sont un [`ExpertId`].
    pub const GET_EXPERT: &str = "get-expert";
    /// Client -> control plane : liste tout le catalogue d'experts.
    pub const LIST_EXPERTS: &str = "list-experts";
    /// Client -> control plane : crée ou remplace la déclaration d'un graphe
    /// d'états (répliqué via Raft, voir
    /// `ControlPlaneRequest::SetStateGraph`). Les arguments sont un
    /// [`SetStateGraphRequest`].
    pub const SET_STATE_GRAPH: &str = "set-state-graph";
    /// Client -> control plane : retire un graphe d'états du catalogue
    /// (répliqué via Raft, voir `ControlPlaneRequest::RemoveStateGraph`). Les
    /// arguments sont un [`StateGraphId`].
    pub const REMOVE_STATE_GRAPH: &str = "remove-state-graph";
    /// Client -> control plane : récupère la déclaration d'un graphe d'états.
    /// Les arguments sont un [`StateGraphId`].
    pub const GET_STATE_GRAPH: &str = "get-state-graph";
    /// Client -> control plane : liste tout le catalogue de graphes d'états.
    pub const LIST_STATE_GRAPHS: &str = "list-state-graphs";
    pub const APPEND_ENTRIES: &str = "append-entries";
    pub const INSTALL_SNAPSHOT: &str = "install-snapshot";
    pub const VOTE: &str = "vote";
    /// Client -> control plane : propose un nouveau job (répliqué via Raft).
    pub const SUBMIT_JOB: &str = "submit-job";
    /// Control plane -> worker : demande d'exécuter le job joint. Best-effort :
    /// l'assignation fait foi dans l'état Raft, cet appel n'est qu'une notification.
    pub const RUN_JOB: &str = "run-job";
    /// Worker -> control plane : rapporte une transition d'état d'un job
    /// (répliquée via Raft).
    pub const REPORT_JOB_STATE: &str = "report-job-state";
    /// Vérificateur -> pair prétendant être `ControlPlane` : défi
    /// d'authentification (voir `secret::SecretManager::prove_membership`).
    /// Les arguments sont un nonce `[u8; 32]`, la réponse la preuve associée.
    pub const AUTH_CHALLENGE: &str = "auth-challenge";
    /// Pair -> control plane : s'enregistre comme exécuteur volontaire du nom
    /// de RPC donné en argument (`String`). Le control plane relaiera ensuite
    /// tout appel portant ce nom vers ce pair (voir `NetworkClient::register_rpc`).
    pub const REGISTER_RPC: &str = "register-rpc";
    /// Worker -> worker : demande le diff CRDT manquant d'une session (voir
    /// `session::crdt::YrsSession`) au pair qui la détient actuellement.
    /// Les arguments sont un [`SessionFetchRequest`], la réponse un diff
    /// `encode_diff_v1` prêt à être appliqué via `YrsSession::apply_diff`.
    pub const FETCH_SESSION: &str = "fetch-session";
    /// Worker/client -> control plane : qui détient (ou pourrait servir de
    /// secours pour) l'état CRDT d'une session, dans l'ordre à essayer (voir
    /// `network::cp::session_holders_for` : les workers l'exécutant
    /// actuellement d'abord, les nœuds `Persistency` connus en dernier
    /// recours). Les arguments sont un [`crate::session::SessionId`], la
    /// réponse un `Vec<libp2p::PeerId>`, potentiellement vide si personne
    /// n'est connu (première prise en charge de cette session) — voir
    /// `session::client::SessionClient::acquire`.
    pub const SESSION_HOLDERS: &str = "session-holders";
    /// Client -> nœud de persistance : supprime définitivement une session
    /// (voir `persistency::SessionStore`) et son `/session/files` (voir
    /// `persistency::vfs::WorkspaceVfs::delete_session_files`). Les
    /// arguments sont un [`SessionId`]. Irréversible : à n'appeler qu'une
    /// fois certain qu'aucun worker n'a plus besoin de cette session.
    pub const DELETE_SESSION: &str = "delete-session";
    /// Worker -> worker : demande le diff CRDT manquant d'un workspace (voir
    /// `workspace::crdt::YrsWorkspace`), sur exactement le même principe que
    /// [`Self::FETCH_SESSION`]. Les arguments sont un [`WorkspaceFetchRequest`],
    /// la réponse un diff `encode_diff_v1` prêt à être appliqué via
    /// `YrsWorkspace::apply_diff`.
    pub const FETCH_WORKSPACE: &str = "fetch-workspace";
    /// Worker/client -> control plane : qui détient (ou pourrait servir de
    /// secours pour) l'état CRDT d'un workspace, sur le même principe que
    /// [`Self::SESSION_HOLDERS`] (voir `network::cp::workspace_holders_for` :
    /// dérivé des workers exécutant actuellement une session membre du
    /// workspace — voir `ControlPlaneState::session_workspaces` — les nœuds
    /// `Persistency` connus en dernier recours). Les arguments sont un
    /// [`WorkspaceId`], la réponse un `Vec<libp2p::PeerId>`, potentiellement
    /// vide (première prise en charge de ce workspace) — voir
    /// `workspace::client::WorkspaceClient::acquire`.
    pub const WORKSPACE_HOLDERS: &str = "workspace-holders";
    /// Worker/client -> control plane : déclare (ou efface, si
    /// `workspace_id` est `None`) le workspace auquel appartient une session
    /// — répliqué via Raft (voir `ControlPlaneRequest::SetSessionWorkspace`
    /// et `ControlPlaneState::session_workspaces`). Une session n'appartient
    /// jamais qu'à un seul workspace à la fois : appeler ceci une seconde
    /// fois avec un `workspace_id` différent remplace silencieusement
    /// l'appartenance précédente. Les arguments sont un
    /// [`SetSessionWorkspaceRequest`].
    pub const SET_SESSION_WORKSPACE: &str = "set-session-workspace";
    /// Worker/client -> control plane : lit le workspace auquel appartient
    /// une session (voir `ControlPlaneState::session_workspaces`), sans le
    /// modifier — contrepartie en lecture de [`Self::SET_SESSION_WORKSPACE`],
    /// utilisée par `session::client::SessionClient` pour résoudre le VFS
    /// d'une session (voir `persistency::vfs::WorkspaceVfs::mount_session`).
    /// Les arguments sont un [`SessionId`], la réponse un
    /// `Option<WorkspaceId>` (`None` si la session n'est rattachée à aucun
    /// workspace, ou inconnue du control plane). Lecture seule, servie depuis
    /// l'état Raft local (voir [`Self::SESSION_HOLDERS`]) : pas besoin d'être
    /// le leader.
    pub const SESSION_WORKSPACE: &str = "session-workspace";
}

impl RpcCall {
    #[must_use]
    pub fn new(name: impl ToString, args: impl Serialize) -> Self {
        Self {
            name: name.to_string(),
            args: serde_json::to_value(args).unwrap()
        }
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub enum RpcResult {
    RpcOk(serde_json::Value),
    RpcErr(String)
}

/// Retour d'une RPC dont l'appelant ne se soucie que du succès/échec
/// transport (voir [`crate::network::actor::NetworkClient::rpc`]), pas du
/// contenu de la réponse : accepte n'importe quelle valeur JSON renvoyée par
/// la cible (`Value::Null`, ou un type de réponse concret ignoré, ex.
/// `ControlPlaneResponse`) sans chercher à la désérialiser en un type précis.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Void;

impl Serialize for Void {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_unit()
    }
}

impl<'de> Deserialize<'de> for Void {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        serde::de::IgnoredAny::deserialize(deserializer)?;
        Ok(Void)
    }
}

/// Rapport de transition d'état d'un job, échangé via [`RpcCall::REPORT_JOB_STATE`]
/// (worker -> control plane).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStateReport {
    pub job_id: JobId,
    pub state: JobState,
}

/// Charge utile de [`RpcCall::SET_MODEL`] (client -> control plane) : `id`
/// est distinct de la clé sous laquelle l'appelant range la déclaration
/// localement, mais c'est bien elle qui sert de clé dans le catalogue
/// répliqué (voir `ControlPlaneRequest::SetModel`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetModelRequest {
    pub id: ModelId,
    pub declaration: Model,
}

/// Charge utile de [`RpcCall::SET_TOOL`] (client -> control plane), sur le
/// même modèle que [`SetModelRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetToolRequest {
    pub id: ToolId,
    pub declaration: ToolDeclaration,
}

/// Charge utile de [`RpcCall::SET_EXPERT`] (client -> control plane), sur le
/// même modèle que [`SetModelRequest`]/[`SetToolRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetExpertRequest {
    pub id: ExpertId,
    pub declaration: ExpertDeclaration,
}

/// Charge utile de [`RpcCall::SET_STATE_GRAPH`] (client -> control plane),
/// sur le même modèle que [`SetModelRequest`]/[`SetToolRequest`]/[`SetExpertRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetStateGraphRequest {
    pub id: StateGraphId,
    pub declaration: StateGraphDeclaration,
}

/// Requête de synchronisation d'une session, échangée via
/// [`RpcCall::FETCH_SESSION`] (worker -> worker). `state_vector` est le
/// vecteur d'état yrs (`StateVector::encode_v1`) du demandeur — vide s'il
/// n'a jamais vu cette session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionFetchRequest {
    pub session_id: SessionId,
    pub state_vector: Vec<u8>,
}

/// Charge utile de [`RpcCall::RUN_JOB`] (control plane -> worker) : le job à
/// exécuter. Le worker retrouve seul les détenteurs actuels de l'état CRDT
/// de la session ciblée (voir [`RpcCall::SESSION_HOLDERS`] et
/// `session::client::SessionClient::acquire`) plutôt que de dépendre d'une
/// liste figée au moment de l'assignation — au cas où elle serait déjà
/// périmée (ex: un détenteur indiqué ici s'est déconnecté entre
/// l'assignation et l'exécution effective de cette RPC).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunJobRequest {
    pub job: Job,
}

/// Requête de synchronisation d'un workspace, échangée via
/// [`RpcCall::FETCH_WORKSPACE`] (worker -> worker) — sur le même principe
/// que [`SessionFetchRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceFetchRequest {
    pub workspace_id: WorkspaceId,
    pub state_vector: Vec<u8>,
}

/// Charge utile de [`RpcCall::SET_SESSION_WORKSPACE`] (worker/client ->
/// control plane).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetSessionWorkspaceRequest {
    pub session_id: SessionId,
    pub workspace_id: Option<WorkspaceId>,
}

pub type Behaviour = request_response::json::Behaviour<RpcCall, RpcResult>;

