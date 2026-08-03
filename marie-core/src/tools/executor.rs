use std::{collections::HashMap, sync::Arc};

use futures::{FutureExt, future::BoxFuture};
use parking_lot::Mutex;
use serde::{Serialize, de::DeserializeOwned};

use crate::{err, session::SessionId, tools::ToolId};

pub type ToolExecutor = Arc<dyn Fn(SessionId, serde_json::Value) -> BoxFuture<'static, Result<serde_json::Value, crate::Error>> + Send + Sync + 'static>;

#[derive(Clone, Default)]
pub struct ToolExecutors(Arc<Mutex<HashMap<ToolId, ToolExecutor>>>);

impl ToolExecutors {
    pub fn add<F, Args, R, Fut>(&self, id: impl Into<ToolId>, executor: F)
        where
            F: Fn(SessionId, Args) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<R, crate::Error>> + Send + 'static,
            R: Serialize,
            Args: DeserializeOwned
    {
        let wrapped = move |session_id: SessionId, args: serde_json::Value| {
            let task = match serde_json::from_value(args) {
                Err(error) => return std::future::ready(Err(err!("échec lors de l'amorçage de l'exécution de l'outil: {error}"))).boxed(),
                Ok(args) => executor(session_id, args)
            };

            async move {
                let result = task.await;

                if let Err(error) = &result {
                    return Err(err!("échec lors de l'exécution de l'outil: {error}"));
                }

                let result = serde_json::to_value(result.unwrap());
                if let Err(error) = &result {
                    return Err(err!("échec lors de la serialization du retour de l'outil: {error}"));
                }

                Ok(result.unwrap())
            }.boxed()
        };

        self.0.lock().insert(id.into(), Arc::new(wrapped));
    }

    pub fn get(&self, id: &ToolId) -> Option<ToolExecutor> {
        self.0.lock().get(id).cloned()
    }
}