use crate::{hitl::dto::PendingHitl, session::{SessionId, SessionStatus, logs::SessionLog}};

pub struct SessionView {
    pub id: SessionId,
    pub status: SessionStatus,
    pub logs: Vec<SessionLog>,
    pub pendings: Vec<PendingHitl>
}