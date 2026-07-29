use serde::{Deserialize, Serialize};

#[derive(Debug, Hash, Clone, Serialize, Deserialize, PartialEq, Eq)]

pub struct Javascript {
    entrypoint: String,
    source: String
}