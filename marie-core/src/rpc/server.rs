use std::{collections::HashMap, sync::Arc};

use futures::{FutureExt as _, future::BoxFuture};
use futures_util::StreamExt;
use libp2p::PeerId;
use parking_lot::Mutex;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::{select, task::JoinHandle};

use crate::{di::{Factory, Get}, node::NodeId, post::PostOffice, rpc::{RpcError, protocol::RpcReply}};
use super::protocol::{RpcMessage, RpcCall};

pub trait RpcServable {
    fn register<F, Args, R, Fut>(&mut self, name: impl ToString, f: F)
        where 
            F: Fn(Args, PeerId) -> Fut + Send + Sync + 'static, 
            Fut: Future<Output = R> + Send + 'static,
            Args: DeserializeOwned, 
            R: Serialize + 'static;
}


#[derive(Clone)]
pub struct RpcServer {
    postoff: PostOffice,
    executors: Arc<Mutex<HashMap<String, RpcExecutor>>>
}

impl<C> Factory<C> for RpcServer where C: Get<PostOffice> {
    fn create(container: &C) -> Self {
        let postoff: PostOffice = container.get();
        Self::new(postoff)
    }
}

impl RpcServer {
    pub fn new(postoff: PostOffice) -> Self {
        let server = RpcServer {
            postoff,
            executors: Arc::new(Mutex::new(HashMap::default()))
        };

        tokio::spawn(server.clone().run());

        server
    }

    async fn run(self) {
        use RpcMessage::Call;

        let mut rx = self.postoff.stream_messages::<RpcMessage>();
        loop {
            select! {
                Some(msg) = rx.next() => {
                    match msg.payload {
                        Call(call) => { tokio::spawn(self.clone().handle_call(call, msg.source)); },
                        _ => {}
                    }
                }
            }
        }
    }

    fn send(&self, msg: impl Into<RpcMessage>, destination: NodeId) -> Result<(), RpcError> {
        self.postoff.send(msg, destination)?;
        Ok(())
    }

    async fn handle_call(self, call: RpcCall, callee: NodeId) {
        let id = call.id;

        let result = self.clone()
            .execute_call(call, callee.clone())
            .await;
        
        let _ = self.send(RpcReply {id, result}, callee);
    }

    async fn execute_call(self, call: RpcCall, callee: NodeId) -> Result<Value, RpcError> {
        let executor = self.executors.lock()
            .get(&call.name)
            .ok_or_else(|| RpcError::NoExecutorFound(call.name.clone()))
            .cloned()?;

        let ret = executor.execute(call.args, callee).await;

        Ok(ret)
    }
}

impl RpcServer {
    /// Enregistre un RPC au nom donné
    pub fn register<F, Args, R, Fut>(&self, name: impl ToString, f: F)
        where 
            F: Fn(Args, NodeId) -> Fut + Send + Sync + 'static, 
            Fut: Future<Output = R> + Send + 'static,
            Args: DeserializeOwned, 
            R: Serialize + 'static
    {
        let name = name.to_string();
        let executor: RpcExecutor = RpcExecutor::new(f);
        self.executors.lock().insert(name.clone(), executor);
    } 
}

struct RpcInfo {
    call: RpcCall,
    sent_at: std::time::Instant,
    retry: u8,
    handle: Option<JoinHandle<()>>
}

#[derive(Clone)]
/// Remote procedure call executor
struct RpcExecutor(Arc<dyn Fn(serde_json::Value, NodeId) -> BoxFuture<'static, serde_json::Value> + Send + Sync>);

impl RpcExecutor {
    pub fn new<F, Args, R, Fut>(f: F) -> Self
        where 
            F: Fn(Args, NodeId) -> Fut + Sync + Send + 'static, 
            Fut: Future<Output = R> + Send + 'static,
            Args: DeserializeOwned, 
            R: Serialize + 'static
    {
        let func = move |args: serde_json::Value, source: NodeId| {
            let args: Args = serde_json::from_value(args).unwrap();
            let fut = f(args, source);

            async move {
                let ret = fut.await;
                serde_json::to_value(&ret).unwrap()
            }.boxed()
        };

        let inner = Arc::new(func);

        Self(inner)
    }

    #[inline]
    pub fn execute(&self, args: serde_json::Value, source: NodeId) -> BoxFuture<'static, serde_json::Value> {
        (&self.0)(args, source)
    }
}
