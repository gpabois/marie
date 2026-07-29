pub mod model;
pub mod protocol;

pub use model::{Question, Answer, QuestionKind};
use serde::{Deserialize, Serialize};

use crate::session::{frames::{FrameData, NewFrame}, snapshot::SnapshotRef};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Hitl {
    Question(Question),
    Text
}

impl From<Hitl> for FrameData {
    fn from(value: Hitl) -> Self {
        FrameData::Hitl(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NewHitlFrame {
    pub snapshot: SnapshotRef,
    pub data: Hitl
}

impl From<NewHitlFrame> for NewFrame {
    fn from(value: NewHitlFrame) -> Self {
        NewFrame::Hitl(value)
    }
}

impl NewHitlFrame {
    pub fn question(question: Question) -> NewHitlFrame {
        NewHitlFrame(Hitl::Question(question))
    }

    pub fn text() -> NewHitlFrame {
        NewHitlFrame(Hitl::Text)
    }
}