use std::{borrow::Borrow, collections::HashMap, fmt::Display};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::session::frames::FrameId;

#[derive(Debug, Hash, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChannelName(String);

impl Borrow<str> for ChannelName {
    #[inline]
    fn borrow(&self) -> &str {
        &self.0[..]
    }
}

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

#[derive(Debug, Clone)]
pub struct ChannelsTracker {
    frame_id: FrameId,
    channels: Channels,
    updates: Vec<ChannelUpdate>
}

impl ChannelsTracker {
    pub fn new(frame_id: FrameId, channels: Channels) -> Self {
        Self {
            frame_id,
            channels,
            updates: vec![]
        }
    }

    pub fn into_updates(self) -> Vec<ChannelUpdate> {
        self.updates
    }
}

impl ChannelsTracker {
    pub fn write(&mut self, name: impl Into<ChannelName>, value: impl Serialize) -> crate::Result<()> {
        let name = name.into();
        let value = serde_json::to_value(value).unwrap();
        self.updates.push(ChannelUpdate { name, value, contributor: self.frame_id });
        Ok(())
    }

    pub fn read<V: DeserializeOwned>(&self, name: impl Into<ChannelName>) -> crate::Result<V> {
        let name = name.into();

        let value = self.updates.iter().rev()
            .find(|update| update.name == name)
            .map(|update| &update.value)
            .cloned()
            .or_else(|| self.channels.get(&name))
            .ok_or_else(|| crate::err!("no channel {name} exists"))?;

        let value = serde_json::from_value(value)?;
        Ok(value)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Channels(HashMap<ChannelName, Value>);

impl Channels {
    pub fn get<Q: ?Sized>(&self, name: &Q) -> Option<Value>
        where ChannelName: Borrow<Q>,
                Q: std::hash::Hash + std::cmp::Eq
    {
        self.0.get(name).cloned()
    }

    pub fn write(&mut self, name: impl Into<ChannelName>, value: impl Serialize) {
        let name = name.into();
        let value = serde_json::to_value(value).unwrap();
        self.0.insert(name, value);
    }

    pub fn extend(&mut self, value: impl IntoIterator<Item=(ChannelName, Value)>) {
        self.0.extend(value)
    }
}

impl FromIterator<(ChannelName, Value)> for Channels {
    fn from_iter<T: IntoIterator<Item = (ChannelName, Value)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}
