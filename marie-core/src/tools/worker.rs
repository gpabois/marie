use std::{collections::HashMap, sync::Arc};

use anyhow::anyhow;
use async_trait::async_trait;
use futures::{FutureExt, future::BoxFuture};
use parking_lot::Mutex;
use serde::{Serialize, de::DeserializeOwned};

use crate::{job::Job, network::worker::JobContext, session::{SessionId, client::SessionClient}, tools::{JOB_TOOL_EXECUTE, ToolCall, ToolCallError, ToolCallResult, ToolId}};

#[cfg(feature = "worker")]
use crate::network::worker::server::WorkerServer;

type ToolExecutor = Arc<dyn Fn(SessionId, serde_json::Value) -> BoxFuture<'static, Result<serde_json::Value, anyhow::Error>> + Send + Sync + 'static>;

#[cfg(feature = "worker")]
#[derive(Default)]
pub struct ToolWorkerArgs(HashMap<ToolId, ToolExecutor>);

#[cfg(feature = "worker")]
impl ToolWorkerArgs {
    pub fn add<F, Args, R, Fut>(mut self, id: impl Into<ToolId>, executor: F) -> Self 
        where 
            F: Fn(SessionId, Args) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<R, anyhow::Error>> + Send + Sync + 'static,
            R: Serialize,
            Args: DeserializeOwned
    {
        let wrapped = move |session_id: SessionId, args: serde_json::Value| {
            let task = match serde_json::from_value(args) {
                Err(error) => return std::future::ready(Err(anyhow!("échec lors de l'amorçage de l'exécution de l'outil: {error}"))).boxed(),
                Ok(args) => executor(session_id, args)
            };

            async move {
                let result = task.await;

                if let Err(error) = &result {
                    return Err(anyhow!("échec lors de l'exécution de l'outil: {error}"));
                }

                let result = serde_json::to_value(result.unwrap());
                if let Err(error) = &result {
                    return Err(anyhow!("échec lors de la serialization du retour de l'outil: {error}"));
                }

                Ok(result.unwrap())
            }.boxed()
        };

        self.0.insert(id.into(), Arc::new(wrapped));
        self
    }
}

#[cfg(feature = "worker")]
pub struct ToolWorker(Arc<Mutex<HashMap<ToolId, ToolExecutor>>>, SessionClient);

#[cfg(feature = "worker")]
impl ToolWorker {
    pub fn new(args: ToolWorkerArgs, sessions: SessionClient) -> Self {
        Self(Arc::new(Mutex::new(args.0)), sessions)
    }

    pub fn register(&self, worker: &mut WorkerServer<JobContext>) {
        ToolExecution(self.0.clone(), self.1.clone()).register(worker);
    }
}

/// Job délégué par [`crate::tools::rpc::ExecuteTool`] : recherche l'exécuteur
/// enregistré pour le nom de tool porté par le [`ToolCall`] et l'exécute,
/// puis rapporte le résultat (succès ou échec) à `SessionServer` via
/// [`SessionClient::report_tool_execution`] — même modèle qu'`RunAgent` qui
/// rapporte son issue en toute fin de `Job` (voir
/// `session::worker::RunAgent`). Rapporté sur *tous* les chemins (y compris
/// "aucun exécuteur trouvé") : un appel jamais rapporté laisserait son
/// identifiant bloqué indéfiniment dans le `tools_calls` du frame appelant.
#[derive(Clone)]
pub struct ToolExecution(Arc<Mutex<HashMap<ToolId, ToolExecutor>>>, SessionClient);

#[async_trait]
impl Job for ToolExecution {
    const NAME: &'static str = JOB_TOOL_EXECUTE;

    type Args = ToolCall;
    type Return = serde_json::Value;

    async fn execute(self, call: ToolCall, _cx: JobContext) -> Result<serde_json::Value, anyhow::Error> {
        let executor = self.0.lock().get(call.name.as_str()).cloned();

        let outcome = match executor {
            Some(executor) => executor(call.id.session_id(), call.parameters.clone()).await,
            None => Err(anyhow!("aucun exécuteur d'outil trouvé pour {}", call.name)),
        };

        let result = match &outcome {
            Ok(value) => ToolCallResult::Success(Some(value.to_string())),
            Err(error) => ToolCallResult::Failed(ToolCallError::Custom(error.to_string())),
        };

        self.1.report_tool_execution(call.agent_id, call.id, result).await?;

        outcome
    }
}