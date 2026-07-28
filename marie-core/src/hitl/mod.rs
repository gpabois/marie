pub mod model;
pub mod protocol;

pub use model::{Question, Answer, QuestionKind};
use serde::{Deserialize, Serialize};

use crate::session::frames::Frame;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Hitl {
    Question(Question),
    Text
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HitlFrame(Hitl);

impl From<HitlFrame> for Frame {
    fn from(value: HitlFrame) -> Self {
        Frame::Hitl(value)
    }
}

impl HitlFrame {
    pub fn question(question: Question) -> HitlFrame {
        HitlFrame(Hitl::Question(question))
    }

    pub fn text() -> HitlFrame {
        HitlFrame(Hitl::Text)
    }
}