use std::{collections::HashMap, sync::Arc};

use futures::{FutureExt as _, SinkExt, future::BoxFuture};
use futures_util::StreamExt;
use libp2p::PeerId;
use parking_lot::Mutex;
use serde::{Serialize, de::DeserializeOwned};
use tokio::{select, sync::mpsc, task::JoinHandle};
use tracing::warn;

use crate::{di::{Factory, Get}, layer::{Layer, LayerExt}, network::{Network, mux::FrameLayer, rpc::RpcMuxLayer}, rpc::{RpcAck, RpcCall, RpcCallId, RpcError, RpcMessage, RpcReply, RpcResult}};



#[derive(Clone)]
pub struct RpcServer {
    executed: Arc<Mutex<Vec<String>>>,
    tx: mpsc::UnboundedSender<Command>
}

impl<C> Factory<C> for RpcServer where C: Get<Network> {
    fn create(container: &C) -> Self {
        let network: Network = container.get();
        let actor = Actor::default();
        actor.run(network.layer()
            .chain::<FrameLayer, _>(())
            .chain::<RpcMuxLayer, _> (())
        )
    }
}

impl RpcServer {
    pub fn can_execute(&self) -> Vec<String> {
        self.executed.lock().clone()
    }
}

impl RpcServer {
    /// Enregistre un RPC au nom donné
    pub fn register<F, Args, R, Fut>(&mut self, name: impl ToString, f: F)
        where 
            F: Fn(Args, PeerId) -> Fut + Send + Sync + 'static, 
            Fut: Future<Output = R> + Send + 'static,
            Args: DeserializeOwned, 
            R: Serialize + 'static
    {
        use Command::Register;

        let name = name.to_string();
        let exe = RpcExecutor::new(f);
        self.executed.lock().push(name.clone());
        let _ = self.tx.send(Register(name, exe));
    } 
}

#[derive(Default)]
struct Actor {
    executors: HashMap<String, RpcExecutor>
}

impl Actor {
    /// Enregistre un RPC au nom donné
    pub fn register<F, Args, R>(&mut self, name: impl ToString, f: F) 
        where 
            F: Fn(Args, PeerId) -> BoxFuture<'static, R> + Send + Sync + 'static, 
            Args: DeserializeOwned, 
            R: Serialize + 'static
    {
        let name = name.to_string();
        let exe = RpcExecutor::new(f);

        self.executors.insert(name, exe);
    }

    pub fn run(self, layer: impl Layer<Send=RpcMessage, Received=RpcMessage>) -> RpcServer 
    {
        let (tx, rx) = layer.split();
        let mut rx = Box::pin(rx);
        let mut tx = Box::pin(tx);
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<Command>();
        let (ev_tx, mut ev_rx) = mpsc::unbounded_channel::<Event>();

        let mut executors = self.executors;
        let executed = executors.iter().map(|(name, _)| name).cloned().collect();

        let cmd_tx_out = cmd_tx.clone();
        tokio::spawn(async move {
            use RpcMessage::Call;
            use Command::Execute;

            loop {
                select! {
                    Some(msg) = rx.next() => {
                        if let Call(call) = msg {
                            cmd_tx_out.send(Execute(call));
                        }
                    }
                }
            }
        });

        tokio::spawn(async move {
            use Event::*;
            use Command::*;
            use RpcMessage::{Ack, Reply};

            let mut ongoings = HashMap::<RpcCallId, RpcInfo>::default();

            loop {
                select! {
                    Some(cmd) = cmd_rx.recv() => {
                        match cmd {
                            Register(name, executor) => {
                                executors.insert(name, executor);
                            },
                            Execute(call) => {
                                // on n'exécute pas plusieurs fois le même call.
                                // si la tâche existe déjà, on la laisse tourner.
                                if let Some(info) = ongoings.get(&call.id) && info.handle.is_some() { continue }
                                if let Some(executor) = executors.get(&call.name).cloned() {
                                    let ack = RpcAck { id: call.id, destination: call.source, source: call.destination };
                                    if let Err(err) = tx.send(Ack(ack)).await {
                                        warn!("échec de l'envoi de l'accusé de réception RPC pour {} : {err}", call.name);
                                    }

                                    let task = executor.execute(call.args.clone(), call.source.unwrap());
                                    let ev_tx_h = ev_tx.clone();
                                    
                                    let call_2 = call.clone();
                                    let handle = tokio::spawn(async move {
                                        let resp = task.await;
                                        let reply = RpcReply { 
                                            id: call.id, 
                                            result: crate::rpc::RpcResult::Ok(resp), 
                                            destination: call.source,
                                            source: call.destination
                                        };
                                        ev_tx_h.send(Finished(reply));
                                    });

                                    let _ = ev_tx.send(Spawned(call_2, handle));
                                } else {
                                    let failed_reply = RpcReply { 
                                        id: call.id, 
                                        result: RpcResult::Error(RpcError::NoExecutorFound), 
                                        destination: call.source, 
                                        source: call.destination 
                                    };

                                    let _ = ev_tx.send(Finished(failed_reply));
                                }
                            }
                        }
                    },
                    Some(ev) = ev_rx.recv() => {
                        match ev {
                            Spawned(rpc_call, join_handle) => {
                                let info = ongoings.entry(rpc_call.id).or_insert_with(|| RpcInfo {
                                    call: rpc_call.clone(),
                                    sent_at: std::time::Instant::now(),
                                    retry: 3,
                                    handle: None
                                });

                                info.handle = Some(join_handle);
                            },
                            Finished(reply) => {
                                ongoings.remove(&reply.id);
                                if let Err(err) = tx.send(Reply(reply)).await {
                                    warn!("échec de l'envoi de la réponse RPC : {err}");
                                }
                            },
                        }
                    }
                }
            }
        });


        RpcServer {
            executed: Arc::new(Mutex::new(executed)),
            tx: cmd_tx.clone()
        }
    }
}

struct RpcInfo {
    call: RpcCall,
    sent_at: std::time::Instant,
    retry: u8,
    handle: Option<JoinHandle<()>>
}

enum Command {
    Execute(RpcCall),
    Register(String, RpcExecutor)
}

enum Event {
    Spawned(RpcCall, JoinHandle<()>),
    Finished(RpcReply)
}

#[derive(Clone)]
/// Remote procedure call executor
struct RpcExecutor(Arc<dyn Fn(serde_json::Value, PeerId) -> BoxFuture<'static, serde_json::Value> + Send + Sync>);

impl RpcExecutor {
    pub fn new<F, Args, R, Fut>(f: F) -> Self
        where 
            F: Fn(Args, PeerId) -> Fut + Sync + Send + 'static, 
            Fut: Future<Output = R> + Send + 'static,
            Args: DeserializeOwned, 
            R: Serialize + 'static
    {
        let func = move |args: serde_json::Value, source: PeerId| {
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
    pub fn execute(&self, args: serde_json::Value, source: PeerId) -> BoxFuture<'static, serde_json::Value> {
        (&self.0)(args, source)
    }
}
