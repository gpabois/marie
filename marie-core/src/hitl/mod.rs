pub mod model;
pub mod protocol;
pub mod dto;
pub mod store;
pub mod service;

use std::{collections::HashMap, fmt, str::FromStr};

pub use model::{Question, Answer, QuestionKind};
use serde::{Deserialize, Serialize};

use crate::{id::ID, session::SessionId};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Hitl {
    Question(Question),
    Text
}

#[derive(Debug, Hash, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct HitlId(pub(crate) ID);

impl From<ID> for HitlId {
    fn from(value: ID) -> Self {
        Self(value)
    }
}

impl fmt::Display for HitlId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for HitlId {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

pub struct HitlRequest {
    pub session_id: SessionId,
    pub id: HitlId,
    pub hitl: Hitl
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HitlFrame {
    pub session_id: SessionId,
    pub id: HitlId,
    pub hitl: Hitl,
    pub answer: Option<HashMap<String, Answer>>
}
