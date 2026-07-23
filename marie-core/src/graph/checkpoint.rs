use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct Checkpoint<S> {
    pub step: u32,
    pub node: String,
    pub state: S,
}

#[async_trait]
pub trait Checkpointer<S>: Send + Sync {
    async fn save(&self, thread_id: &str, checkpoint: Checkpoint<S>) -> anyhow::Result<()>;
    async fn latest(&self, thread_id: &str) -> anyhow::Result<Option<Checkpoint<S>>>;
    async fn history(&self, thread_id: &str) -> anyhow::Result<Vec<Checkpoint<S>>>;
}
 