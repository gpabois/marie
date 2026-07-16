use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::agent::role::Role;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Context(Vec<ContextEntry>);

impl ToString for Context {
    fn to_string(&self) -> String {
        self.0.iter().map(ToString::to_string).collect()
    }
}

impl Deref for Context {
    type Target = [ContextEntry];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Context {
    pub fn push(&mut self, entry: ContextEntry) {
        self.0.push(entry)
    }

}

impl From<Vec<ContextEntry>> for Context {
    fn from(entries: Vec<ContextEntry>) -> Self {
        Self(entries)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextEntry {
    pub role: Role,
    pub content: String
}

impl ToString for ContextEntry {
    fn to_string(&self) -> String {
        self.content.clone()
    }
}
