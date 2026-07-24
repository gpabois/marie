use std::ops::Deref;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type SchemaId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    id: SchemaId,
    schema: Value
}

impl Deref for Schema {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        &self.schema
    }
}
