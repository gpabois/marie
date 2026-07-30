use serde::{Deserialize, Serialize};

use crate::session::channel::{ChannelName, ChannelSpec, Reducer};

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct CommonSpec {
    pub budget: Budget,
    pub channels: Vec<ChannelSpec>,
    pub inherited_channels: Vec<ChannelName>,
    pub exported_channels: Vec<ChannelName>
}

impl CommonSpec {
    /// Fixe temporaire en attendant de 
    /// modifier Expert en ExpertSpec
    pub fn expert() -> CommonSpec {
        CommonSpec { 
            channels: vec![
                ChannelSpec::new("task", Reducer::LastWriteWins),
                ChannelSpec::new("history", Reducer::LastWriteWins),
                ChannelSpec::new("answer", Reducer::LastWriteWins)
            ], 
            inherited_channels: vec!["task".into()], 
            exported_channels: vec!["answer".into()],
            ..Default::default()
        }
    }
}


#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Budget {
    pub max_run: Option<u32>
}