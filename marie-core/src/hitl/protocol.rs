use serde::{Deserialize, Serialize};

use crate::{hitl::{Hitl, HitlId, model::Answers}, session::SessionId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HitlRequest {
    pub session_id: SessionId,
    pub id: HitlId,
    pub hitl: Hitl
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HitlResponse {
    pub session_id: SessionId,
    pub id: HitlId,
    pub answers: Answers
}