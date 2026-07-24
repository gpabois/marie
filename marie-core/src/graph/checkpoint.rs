use async_trait::async_trait;

use crate::state::State;

#[derive(Debug, Clone)]
pub struct Checkpoint {
    pub step: u32,
    pub node: String,
    pub state: State,
}

#[async_trait]
pub trait Checkpointer: Send + Sync {
    async fn save(&self, thread_id: &str, checkpoint: Checkpoint) -> anyhow::Result<()>;
    async fn latest(&self, thread_id: &str) -> anyhow::Result<Option<Checkpoint>>;
    async fn history(&self, thread_id: &str) -> anyhow::Result<Vec<Checkpoint>>;
}
 