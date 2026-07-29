use crate::script::Javascript;

pub struct ChannelName(String);

impl From<&str> for ChannelName {
    pub fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for ChannelName {
    pub fn from(value: String) -> Self {
        Self(value)
    }
}


pub struct ChannelSpec {
    name: ChannelName,
    reducer: Reducer,
}


pub enum Reducer {
    LastWriteWins,
    Append,
    Max,
    Script(Javascript)
}