mod client;
pub mod model;
pub mod rpc;
#[cfg(feature = "catalog")]
pub mod server;
pub(crate) mod store;

pub use model::{ExpertId, Expert};
pub use client::ExpertClient;

use crate::agent::Context;

pub const NS_EXPERT: &str = "/marie/ns/experts";

pub struct SpawnExpertArgs {
    expert_id: ExpertId,
    task: Context
}


