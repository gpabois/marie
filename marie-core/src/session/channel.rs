use serde::{Deserialize, Serialize};

use crate::script::Javascript;

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

#[derive(Debug, Hash, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Reducer {
    LastWriteWins,
    Append,
    Max,
    Script(Javascript)
}