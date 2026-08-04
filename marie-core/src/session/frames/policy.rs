use serde::{Deserialize, Serialize};

use crate::{graph::{GraphRef, NodeId}, session::protocol::Branch};


#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FramePolicy {
    pub on_child_failed: ChildFailurePolicy,
    pub resume_policy: ResumePolicy,
    pub reduce_policy: ReducePolicy,
    pub on_resume_policy: OnResumePolicy,
    pub on_start_policy: OnStartPolicy
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReducePolicy {
    #[default]
    DontReduce,
    Reduce
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OnStartPolicy {
    #[default]
    RunFrame,
    AppendGraphThread {
        graph_ref: GraphRef,
        start: NodeId
    },
    /// Append a graph span 
    AppendGraphSpan {
        graph_ref: GraphRef,
        join: NodeId,
        branches: Vec<Branch>
    },
    AppendGraphNode {
        graph_ref: GraphRef,
        start: NodeId
    }
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]

pub enum OnResumePolicy {
    #[default]
    RunFrame,
    MarkComplete
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChildFailurePolicy {
    FailIfAtLeastHasFailed(usize),
    #[default]
    DontFail
}

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResumePolicy {
    #[default]
    // Reduce only on the last terminated child frame
    Sequential,
    // Wait for Hitl
    Hitl,
    // Reduce when all the child frames have terminated.
    FanIn,
}
