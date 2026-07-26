use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::workspace::WorkspaceId;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourcePath(Vec<String>);

#[derive(Debug, Clone)]
pub struct Resource {
    workspace_id: WorkspaceId,
    data: Value
}