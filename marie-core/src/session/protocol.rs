use serde::{Deserialize, Serialize};

use crate::{graph::NodeId, hitl::protocol::HitlRequest, session::frames::FrameId};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FrameResponse {
    pub frame_id: FrameId,
    pub result: FrameResult,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FrameResult {
    RunExhausted,
    Yield(),
    /// Commandes liées au système de graphes
    ExecuteGraphCommand(GraphCommand),
    Completed
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum GraphCommand {
    Fork(Vec<NodeId>),
    GoTo(NodeId),
    Finished
}

pub enum SessionEvent {
    HitlRequested(HitlRequest)
}