pub mod catalog;
pub mod client;
pub mod model;
pub mod rpc;
#[cfg(feature = "catalog")]
pub mod server;

pub use model::{ExpertId, Expert};
pub use rpc::{GetExpert, InsertExpert, ListExpert, RemoveExpert, UpdateExpert};

use crate::agent::Context;

pub const NS_EXPERT: &str = "/marie/ns/experts";

pub struct SpawnExpertArgs {
    expert_id: ExpertId,
    task: Context
}

