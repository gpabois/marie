use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::agent::role::Role;

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Context(Vec<ContextEntry>);

impl ToString for Context {
    fn to_string(&self) -> String {
        self.0.iter().map(ToString::to_string).collect()
    }
}

impl AsRef<[ContextEntry]>for Context {
    fn as_ref(&self) -> &[ContextEntry] {
        &self.0
    }
}

impl Deref for Context {
    type Target = Vec<ContextEntry>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Context {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Vec<ContextEntry>> for Context {
    fn from(entries: Vec<ContextEntry>) -> Self {
        Self(entries)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextEntry {
    pub role: Role,
    pub content: String
}

impl ContextEntry {
    pub fn assistant(content: impl ToString) -> Self {
        Self {
            role: Role::Assistant,
            content: content.to_string()
        }
    }
}

impl ToString for ContextEntry {
    fn to_string(&self) -> String {
        self.content.clone()
    }
}
