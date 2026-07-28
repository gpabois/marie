use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{hitl::{Answer, Hitl}, session::frames::FrameId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HitlRequest {
    id: FrameId,
    request: Hitl
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HitlResponse {
    id: FrameId,
    response: HashMap<String, Answer>
}