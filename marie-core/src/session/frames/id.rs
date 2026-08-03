use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::id::{ID, generate_id};

#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameId(ID);

impl FrameId {
    pub fn new() -> Self {
        Self(generate_id())
    }
}

impl fmt::Display for FrameId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for FrameId {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}
