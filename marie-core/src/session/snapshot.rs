use crate::session::frames::FrameId;

pub struct SnapshotRef {
    frame: FrameId,
    superstep: u64
}

pub struct Snapshot {
    reference: SnapshotRef,
    parent: Option<SnapshotRef>,
    channels: HashMap<String, serde_json::Value>,
    created_at: chrono::DateTime<Utc>,
    pin_count: u32
}