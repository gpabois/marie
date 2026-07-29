use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::session::channel::ChannelSpec;

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct CommonSpec {
    pub budget: Budget,
    pub channels: HashMap<String, ChannelSpec>,
    pub inherited_channels: Vec<String>,
    pub exported_channels: Vec<String>
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Budget {
    pub max_run: Option<u32>
}