use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use strum_macros::EnumDiscriminants;

use crate::{agent::{Context, frame::AgentFrame}, graph::{GraphFrame, GraphId, GraphSpanFrame, GraphSpec, GraphSpecRef, GraphThreadFrame, NodeId, edge::EdgeSpec, frames::{Graph, GraphSpan, GraphThread}, graph::NodeSpec}, hitl::{Hitl, NewHitlFrame}, id::{ID, generate_id}, job::JobState, session::{channel::ChannelName, protocol::FrameResponse, snapshot::{self, SnapshotRef}, spec::CommonSpec}};

#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameId(ID);

impl FrameId {
    pub fn new() -> Self {
        Self(generate_id())
    }
}

#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrameSpec {
    Graph(GraphSpec),
    GraphNode(NodeSpec),
    GraphEdge(EdgeSpec)
}


#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrameSpecRef {
    Graph(GraphSpecRef),
    Hitl,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrameStatus {
    #[default]
    Pending,
    RunCompleted(FrameResponse),
    RunFailed(String),
    Failed(String),
    Completed,
    Ready,
    Running(JobState),
    RunExhausted,
    WaitingChildren
}

pub struct NewFrameNodeArgs<D: Into<FrameData>> {
    id: FrameId, 
    data: D,
    snapshot: SnapshotRef,
    superstep: u32
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameNode {
    pub id: FrameId,
    pub status: FrameStatus,
    pub data: FrameData,
    pub snapshot: SnapshotRef,
    pub inherited_channels: BTreeMap<ChannelName, Value>,
    pub superstep: u32,
    parent: Option<FrameId>,
    next_sibling: Option<FrameId>,
    prev_sibling: Option<FrameId>,
    children: Vec<FrameId>,
}

impl FrameNode {
    pub fn is_waiting_children(&self) -> bool {
        matches!(self.status, FrameStatus::WaitingChildren)
    }

    pub fn has_terminated(&self) -> bool {
        matches!(self.status, FrameStatus::Failed(_) | FrameStatus::Completed)
    }

    pub fn iter_children(&self) -> impl Iterator<Item=FrameId> {
        self.children.iter().cloned()
    }

    pub fn kind(&self) -> FrameKind {
        FrameKind::from(&self.data)
    }

    pub fn spec(&self) -> FrameSpecRef {
        self.data.sec()
    }
}

impl FrameNode {
    pub fn new<D>(args: NewFrameNodeArgs) -> Self 
        where D: Into<FrameData>
    {
        Self {
            id: args.id,
            data: args.data.into(),
            snapshot: args.snapshot,
            status: FrameStatus::default(),
            inherited_channels: BTreeMap::default(),
            superstep: u32,
            parent: None,
            next_sibling: None,
            prev_sibling: None,
            children: vec![],
        }
    }
}


pub enum NewFrame {
    Graph(GraphFrame),
    GraphSpan(GraphSpanFrame),
    GraphThread(GraphThreadFrame),
    Hitl(NewHitlFrame),
}

impl NewFrame {
    fn into_node(self, id: FrameId) -> FrameNode {
        match self {
            NewFrame::Graph(frame) => {
                FrameNode::new(NewFrameNodeArgs {
                    id,
                    data: frame.data,
                    snapshot: frame.snapshot,
                })
            },
            NewFrame::GraphSpan(frame) => {
                FrameNode::new(NewFrameNodeArgs {
                    id,
                    data: frame.data,
                    snapshot: frame.snapshot
                })
            },
            NewFrame::GraphThread(frame) => {
                FrameNode::new(NewFrameNodeArgs {
                    id,
                    data: frame.data,
                    snapshot: frame.snapshot
                })
            },
            NewFrame::Hitl(frame) => {
                FrameNode::new(NewFrameNodeArgs {
                    id,
                    data: frame.data,
                    snapshot: frame.snapshot
                })
            },
        }
    }


}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, EnumDiscriminants)]
#[strum_discriminants(name(FrameKind))]
pub enum FrameData {
    Graph(Graph),
    GraphSpan(GraphSpan),
    GraphThread(GraphThread),
    Hitl(Hitl)
}

impl FrameData {
    pub fn spec(&self) -> FrameSpecRef {
        match self {
            FrameData::Graph(graph) => graph.spec.into(),
            FrameData::GraphSpan(graph_span) => graph_span.spec.into(),
            FrameData::GraphThread(graph_thread) => graph_thread.spec.into(),
            FrameData::Hitl(hitl) => FrameSpecRef::Hitl,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameTree {
    pub root: Option<FrameId>,
    pub arena: HashMap<FrameId, FrameNode>
}

impl FrameTree {
    pub fn set_root(&mut self, frame: impl Into<NewFrame>) -> FrameId {
        let id = self.create_node(frame);
        self.root = Some(id);
        id
    }

    pub fn insert(&mut self, parent: &FrameId, frame: impl Into<NewFrame>, position: usize) -> FrameId {
        let id = self.create_node(frame);
        self.insert_child(parent, &id, position);
        id
    }

    pub fn append(&mut self, parent: &FrameId, frame: impl Into<NewFrame>) -> FrameId {
        let position = self
            .arena
            .get(parent)
            .map(|node| node.children.len())
            .unwrap_or(0);
        self.insert(parent, frame, position)
    }

    pub fn get<'tree>(&'tree self, id: &FrameId) -> &'tree FrameNode {
        self.try_get(id).expect("frame not found")
    }

    pub fn get_mut<'tree>(&'tree mut self, id: &FrameId) -> &'tree mut FrameNode {
        self.try_get_mut(id).expect("frame not found")
    }

    pub fn try_get<'tree>(&'tree self, id: &FrameId) -> Option<&'tree FrameNode> {
        self.arena.get(id)
    }

    pub fn try_get_mut<'tree>(&'tree mut self, id: &FrameId) -> Option<&'tree mut FrameNode> {
        self.arena.get_mut(id)
    }

    pub fn parent_of(&self, id: &FrameId) -> Option<FrameId> {
        self.arena.get(id).and_then(|node| node.parent)
    }

    pub fn next_sibling_of(&self, id: &FrameId) -> Option<FrameId> {
        self.arena.get(id).and_then(|node| node.next_sibling)
    }

    pub fn prev_sibling_of(&self, id: &FrameId) -> Option<FrameId> {
        self.arena.get(id).and_then(|node| node.prev_sibling)
    }

    pub fn iter_children_of<'a>(&'a self, parent: &FrameId) -> Children<'a> {
        Children {
            tree: self,
            parent: *parent,
            index: 0,
        }
    }

    pub fn iter_next_siblings<'a>(&'a self, id: &FrameId) -> NextSiblings<'a> {
        NextSiblings {
            tree: self,
            current: self.next_sibling_of(id),
        }
    }

    pub fn iter_prev_siblings<'a>(&'a self, id: &FrameId) -> PrevSiblings<'a> {
        PrevSiblings {
            tree: self,
            current: self.prev_sibling_of(id),
        }
    }
}

impl FrameTree {
    fn create_node(&mut self, frame: impl Into<NewFrame>) -> FrameId {
        let id = FrameId::new();
        let frame = frame.into();
        let node = frame.into_node(id);
        self.arena.insert(id, node);
        id
    }

    fn delete(&mut self, id: &FrameId) {
        self.detach(id);

        let children = self
            .arena
            .get(id)
            .map(|node| node.children.clone())
            .unwrap_or_default();

        for child in &children {
            self.delete(child);
        }

        self.arena.remove(id);
    }

    fn insert_child(&mut self, parent: &FrameId, child: &FrameId, index: usize) {
        self.detach(child);

        let Some(parent_node) = self.arena.get(parent) else {
            return;
        };
        let index = index.min(parent_node.children.len());
        let prev = index.checked_sub(1).and_then(|i| parent_node.children.get(i).copied());
        let next = parent_node.children.get(index).copied();

        if let Some(prev) = prev {
            self.link_siblings(&prev, child);
        } else if let Some(child_node) = self.arena.get_mut(child) {
            child_node.prev_sibling = None;
        }

        if let Some(next) = next {
            self.link_siblings(child, &next);
        } else if let Some(child_node) = self.arena.get_mut(child) {
            child_node.next_sibling = None;
        }

        if let Some(parent_node) = self.arena.get_mut(parent) {
            parent_node.children.insert(index, *child);
        }

        if let Some(child_node) = self.arena.get_mut(child) {
            child_node.parent = Some(*parent);
        }
    }

    /// Détache le noeud de son parent et de ses siblings, l'isolant du reste de l'arbre
    /// sans le retirer de l'arène. Si le noeud était la racine, celle-ci est réinitialisée.
    fn detach(&mut self, id: &FrameId) {
        self.detach_from_siblings(id);
        self.detach_from_parent(id);

        if self.root.as_ref() == Some(id) {
            self.root = None;
        }
    }

    fn detach_from_parent(&mut self, id: &FrameId) {
        let Some(parent_id) = self.arena.get(id).and_then(|node| node.parent) else {
            return;
        };

        if let Some(parent_node) = self.arena.get_mut(&parent_id) {
            parent_node.children.retain(|child| child != id);
        }

        if let Some(node) = self.arena.get_mut(id) {
            node.parent = None;
        }
    }

    fn detach_from_siblings(&mut self, id: &FrameId) {
        let Some(node) = self.arena.get(id) else {
            return;
        };
        let (prev, next) = (node.prev_sibling, node.next_sibling);

        if let Some(prev_id) = prev {
            if let Some(prev_node) = self.arena.get_mut(&prev_id) {
                prev_node.next_sibling = next;
            }
        }

        if let Some(next_id) = next {
            if let Some(next_node) = self.arena.get_mut(&next_id) {
                next_node.prev_sibling = prev;
            }
        }

        if let Some(node) = self.arena.get_mut(id) {
            node.prev_sibling = None;
            node.next_sibling = None;
        }
    }

    fn link_siblings(&mut self, prev: &FrameId, next: &FrameId) {
        if let Some(prev_node) = self.arena.get_mut(prev) {
            prev_node.next_sibling = Some(*next);
        }

        if let Some(next_node) = self.arena.get_mut(next) {
            next_node.prev_sibling = Some(*prev);
        }
    }
}

pub struct Children<'a> {
    tree: &'a FrameTree,
    parent: FrameId,
    index: usize
}

impl<'a> Iterator for Children<'a> {
    type Item = FrameId;

    fn next(&mut self) -> Option<Self::Item> {
        let child = self
            .tree
            .arena
            .get(&self.parent)
            .and_then(|node| node.children.get(self.index).copied());

        if child.is_some() {
            self.index += 1;
        }

        child
    }
}

pub struct PrevSiblings<'a> {
    tree: &'a FrameTree,
    current: Option<FrameId>
}

impl<'a> Iterator for PrevSiblings<'a> {
    type Item = FrameId;

    fn next(&mut self) -> Option<Self::Item> {
        let curr = self.current.clone();

        if let Some(curr) = curr {
            self.current = self.tree.prev_sibling_of(&curr);
        }

        curr
    }
}

pub struct NextSiblings<'a> {
    tree: &'a FrameTree,
    current: Option<FrameId>
}

impl<'a> Iterator for NextSiblings<'a> {
    type Item = FrameId;

    fn next(&mut self) -> Option<Self::Item> {
        let curr = self.current.clone();

        if let Some(curr) = curr {
            self.current = self.tree.next_sibling_of(&curr);
        }

        curr
    }
}
