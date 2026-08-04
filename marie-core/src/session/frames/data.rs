use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{expert::{ExpertAskId, ExpertId}, graph::{GraphRef, NodeId}, hitl::{Hitl, HitlId}, shell::ShellMode, tools::{ToolCallId, ToolName}};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag="kind")]
pub enum FrameKind {
    Void,
    Shell(ShellMode),
    Hitl(HitlId),
    ExpertConsultation,
    ToolCall {
        id: ToolCallId,
        name: ToolName,
        parameters: Value,
    },
    Graph {
        graph_ref: GraphRef,
    },
    GraphNode {
        graph_ref: GraphRef,
        node_id: NodeId
    },
    GraphThread {
        graph_ref: GraphRef,
    },
    GraphSpan {
        graph_ref: GraphRef,
        join: NodeId
    }
}
