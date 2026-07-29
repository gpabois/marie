use crate::{graph::{GraphSpecRef, NodeId}, session::{frames::FrameData, snapshot::{self, SnapshotRef}}};


#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewGraphFrame {
    pub data: Graph,
    pub snapshot: SnapshotRef
}

impl NewGraphFrame {
    pub fn new(spec: GraphSpecRef, snapshot: SnapshotRef) -> Self {
        Self { data: Graph::new(spec), snapshot }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Graph {
    pub spec: GraphSpecRef,
}

impl Graph {
    pub fn new(spec: GraphSpecRef) -> Self {
        Self { spec }
    }
}

impl From<Graph> for FrameData {
    fn from(value: Graph) -> Self {
        FrameData::Graph(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// A frame holding a set of threads (between fork and join)
pub struct NewGraphSpanFrame {
    pub data: GraphSpan,
    pub snapshot: SnapshotRef
}

impl NewGraphSpanFrame {
    pub fn new(spec: GraphSpecRef, snapshot: SnapshotRef) -> Self {
        Self { data: GraphSpan::new(spec), snapshot }
    }
}

impl From<NewGraphSpanFrame> for Frame {
    fn from(value: NewGraphSpanFrame) -> Self {
        Frame::GraphSpan(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphSpan {
    pub spec: GraphSpecRef,
}

impl GraphSpan {
    pub fn new(spec: GraphSpecRef) -> Self {
        Self { spec }
    }
}

impl From<GraphSpan> for FrameData {
    fn from(value: GraphSpan) -> Self {
        FrameData::GraphSpan(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewGraphThreadFrame {
    pub data: GraphThread,
    pub snapshot: SnapshotRef
}

impl NewGraphThreadFrame {
    pub fn new(start: NodeId, spec: GraphSpecRef, snapshot: SnapshotRef) -> Self {
        Self {
            data: GraphThread::new(start, spec),
            snapshot
        }
    }
}

impl From<NewGraphThreadFrame> for Frame {
    fn from(value: NewGraphThreadFrame) -> Self {
        Frame::GraphThread(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphThread {
    pub cursor: NodeId,
    pub spec: GraphSpecRef,
}

impl GraphThread {
    pub fn new(cursor: NodeId, spec: GraphSpecRef) -> Self {
        Self { cursor, spec }
    }
}

impl From<GraphThread> for FrameData {
    fn from(value: GraphThread) -> Self {
        FrameData::GraphThread(value)
    }
}