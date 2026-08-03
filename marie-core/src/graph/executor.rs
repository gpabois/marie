use std::{collections::HashMap, sync::Arc};

use futures::{FutureExt, future::BoxFuture};
use parking_lot::Mutex;

use crate::{graph::{NodeKindId, node::NodeContext}, session::protocol::FrameResult};

/// Sur le même principe que [`crate::tools::executor::ToolExecutor`] : le
/// contexte est pris et rendu par valeur (plutôt que par référence mutable
/// comme [`crate::graph::node::Nodable::execute`]) pour que la future
/// stockée ici n'emprunte rien et reste `'static`, condition nécessaire pour
/// être conservée dans le registre type-effacé ci-dessous.
///
/// Résout en [`FrameResult`] et non [`crate::session::protocol::FrameResponse`] :
/// `frame_id`/`updates` sont des métadonnées de frame que seul l'appelant
/// (qui sait quelle frame est en cours d'exécution) peut renseigner, pas la
/// node elle-même — voir [`crate::graph::node::Nodable::execute`].
pub type NodeExecutor = Arc<dyn Fn(NodeContext) -> BoxFuture<'static, crate::Result<(NodeContext, FrameResult)>> + Send + Sync + 'static>;

#[derive(Clone, Default)]
pub struct NodeExecutors(Arc<Mutex<HashMap<NodeKindId, NodeExecutor>>>);

impl NodeExecutors {
    pub fn add<F, Fut>(&self, id: impl Into<NodeKindId>, executor: F)
        where
            F: Fn(NodeContext) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = crate::Result<(NodeContext, FrameResult)>> + Send + 'static
    {
        let wrapped = move |ctx: NodeContext| executor(ctx).boxed();
        self.0.lock().insert(id.into(), Arc::new(wrapped));
    }

    pub fn get(&self, id: &NodeKindId) -> Option<NodeExecutor> {
        self.0.lock().get(id).cloned()
    }
}
