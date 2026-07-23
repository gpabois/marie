pub mod node;
pub mod edge;
pub mod checkpoint;
pub mod graph;
pub mod server;

pub use node::NodeId;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{id::ID, session::SessionId};

pub type GraphId = String;

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct GraphInstanceId(SessionId, ID);

pub struct ThreadFrame<S> {
    state: S
}

#[derive(Clone)]
pub struct GraphFrame<S: Clone + Serialize + DeserializeOwned> {
    /// The instance id of the Frame
    id: GraphInstanceId,
    /// The graph id
    graph_id: GraphId,
    state: S
}
