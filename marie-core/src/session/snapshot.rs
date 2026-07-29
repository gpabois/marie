use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::session::frames::FrameId;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotRef {
    frame: FrameId,
    superstep: u64
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Snapshot {
    reference: SnapshotRef,
    parent: Option<SnapshotRef>,
    channels: HashMap<String, serde_json::Value>,
    created_at: DateTime<Utc>,
    pin_count: u32
}