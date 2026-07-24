use std::{collections::HashMap, sync::Arc};
use serde::{Deserialize, Serialize};

use crate::{condition::Condition, state::State};

use super::NodeId;

pub type RouterFn<S> = Arc<dyn Fn(&S) -> NodeId + Send + Sync>;

pub trait Reducer: Send + Sync {
    fn reduce(&self, base: State, branch_results: Vec<State>) -> State;
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Router(pub HashMap<NodeId, Condition>);

impl Router {
    pub fn route(&self, state: &State) -> Vec<NodeId> {
        self.0
            .iter()
            .filter(|(id, cond)| cond.check(state))
            .map(|(id, _)| *id)
            .collect()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum Edge {
    /// Transition inconditionnelle vers une node fixe.
    Direct(NodeId),
    /// Transition décidée dynamiquement à partir de l'état courant.
    Conditional(Router),
}