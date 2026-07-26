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
pub(crate) mod protocol;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::session::SessionId;

use protocol::WorkspaceEvent;

pub use model::{Workspace, WorkspaceId};
pub use rpc::{AddSession, GetWorkspace, InsertWorkspace, ListWorkspace, PatchVars, QueryVars, RemoveSession, RemoveWorkspace};

pub const NS_WORKSPACE: &str = "/marie/ns/workspaces";


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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceVarsRemoveRequest {
    pub workspace_id: WorkspaceId,
    pub path: String,
}

