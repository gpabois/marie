use std::{collections::HashMap, fmt::Display};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::session::frames::FrameId;

#[derive(Debug, Hash, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChannelName(String);

impl Display for ChannelName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

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

    pub fn name(&self) -> &ChannelName {
        &self.name
    }

    pub fn reducer(&self) -> &Reducer {
        &self.reducer
    }
}

#[derive(Debug, Hash, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Reducer {
    LastWriteWins,
    Append,
    Max,
}

impl Reducer {
    pub fn reduce(&self, current: Option<Value>, contributions: &[Value]) -> Value {
        match self {
            Reducer::LastWriteWins => contributions.last().cloned().or(current).unwrap_or(Value::Null),
            Reducer::Append => {
                let mut vec = match current {
                    Some(Value::Array(vec)) => vec,
                    _ => vec![]
                };
                vec.extend_from_slice(contributions);
                Value::Array(vec)
            }
            Reducer::Max => {
                let mut best = current.and_then(|v| v.as_f64());
                for c in contributions {
                    if let Some(n) = c.as_f64() {
                        best = Some(best.map_or(n, |b| b.max(n)));
                    }
                }
                best.map(|n| serde_json::json!(n)).unwrap_or(Value::Null)
            }
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChannelUpdate {
    pub name: ChannelName,
    pub value: Value,
    pub contributor: FrameId
}

#[derive(Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Channels(HashMap<ChannelName, Value>);

impl FromIterator<(ChannelName, Value)> for Channels {
    fn from_iter<T: IntoIterator<Item = (ChannelName, Value)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}
