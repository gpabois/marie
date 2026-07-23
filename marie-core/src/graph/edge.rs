use std::sync::Arc;

use super::NodeId;

pub type RouterFn<S> = Arc<dyn Fn(&S) -> NodeId + Send + Sync>;

pub trait Reducer<S>: Send + Sync {
    fn reduce(&self, base: S, branch_results: Vec<S>) -> S;
}

pub enum Edge<S> {
    /// Transition inconditionnelle vers une node fixe.
    Direct(NodeId),
    /// Transition décidée dynamiquement à partir de l'état courant.
    Conditional(RouterFn<S>),
    /// Fan-out : exécute chaque branche en concurrence (à partir d'une
    /// copie de l'état courant) jusqu'à ce qu'elle atteigne `join`, puis
    /// fusionne les résultats via `reducer` avant de continuer sur `join`.
    Fanout {
        branches: Vec<NodeId>,
        join: NodeId,
        reducer: Arc<dyn Reducer<S>>,
    },
}