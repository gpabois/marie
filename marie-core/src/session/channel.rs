use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{script::Javascript, session::frames::FrameId};

#[derive(Debug, Hash, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChannelName(String);

impl From<&str> for ChannelName {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for ChannelName {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[derive(Debug, Hash, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChannelSpec {
    name: ChannelName,
    reducer: Reducer,
}

impl ChannelSpec {
    pub fn new(name: impl Into<ChannelName>, reducer: Reducer) -> Self {
        Self { name: name.into(), reducer }
    }
}

#[derive(Debug, Hash, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Reducer {
    LastWriteWins,
    Append,
    Max,
    Script(Javascript)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChannelUpdate {
    pub name: ChannelName,
    pub value: Value,
    pub contributor: FrameId
}