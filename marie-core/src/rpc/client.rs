use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::{SinkExt as _, StreamExt};
use libp2p::PeerId;
use serde::{Serialize, de::DeserializeOwned};
use tokio::{select, sync::{mpsc, oneshot}};
use tracing::warn;
use typed_builder::TypedBuilder;

use crate::{id::IdGenerator, layer::Layer, rpc::{RemoteProcedureCall, RpcAck, RpcCall, RpcCallId, RpcError, RpcMessage, RpcReply, RpcResult, register::RpcRegistry}};

struct RpcHandler {
    sent_at: std::time::Instant,
    tx: oneshot::Sender<RpcResult>
}

#[derive(Default)]
pub struct RpcClientActor;

struct RpcRequest {
    id: RpcCallId,
    call: RpcCall,
    tx: oneshot::Sender<RpcResult>
}

enum RpcCommand {
    Execute(RpcRequest),
    Shutdown
}

enum RpcEvent {
    OnReply(RpcReply),
    OnAck(RpcAck),
    OnRequest(RpcRequest),
    Shutdown
}

impl RpcClientActor {
    pub fn run(self, layer: impl Layer<Send=RpcMessage, Received=RpcMessage>) -> RpcClient 
    {   
        let (tx, rx) = layer.split();
        let mut outcoming = Box::pin(tx);
        let mut incoming = Box::pin(rx);

        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<RpcCall>();
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<RpcCommand>();
        let (ev_tx, mut ev_rx) = mpsc::unbounded_channel::<RpcEvent>();

        let ev_tx_1 = ev_tx.clone();
        let outcoming_task = tokio::spawn(async move {
            use RpcMessage::Call;

            loop {
                select! {
                    Some(call) = out_rx.recv() => {
                        let id = call.id;

                        if call.destination.is_none() {
                            warn!("no RPC server found to execute {}", call.name);
                                let reply = RpcReply {
                                    id,
                                    destination: None,
                                    source: None,
                                    result: RpcResult::Error(RpcError::NoExecutorFound)
                                };
                                
                                let _ = ev_tx_1.send(RpcEvent::OnReply(reply));
                                continue;
                        }


                        let id = call.id;
                        
                        match outcoming.send(Call(call)).await {
                            Err(_) => {
                                let reply = RpcReply {
                                    id,
                                    destination: None,
                                    source: None,
                                    result: RpcResult::Error(RpcError::Custom(String::default()))
                                };
                                
                                let _ = ev_tx_1.send(RpcEvent::OnReply(reply));
                            },
                            _ => {}
                        }
                    }
                }
            }
        });

        let ev_tx_2 = ev_tx.clone();
        let incoming_task = tokio::spawn(async move {
            loop {
                select! {
                    Some(msg) = incoming.next() => {
                        match msg {
                            RpcMessage::Reply(reply) => {
                                let _ = ev_tx_2.send(RpcEvent::OnReply(reply));
                            },
                            RpcMessage::Ack(ack) => {
                                let _ = ev_tx_2.send(RpcEvent::OnAck(ack));
                            },
                            RpcMessage::Call(_) => {
                                // le client ne reçoit jamais d'appel entrant
                            }
                        }
                    }
                }
            }
        });

        tokio::spawn(async move {
            let mut ongoings = HashMap::<RpcCallId, RpcHandler>::default();
            let timeout = Duration::from_secs(30);
            let mut interval = tokio::time::interval(Duration::from_millis(10));

            loop {
                select! {
                    _ = interval.tick() => {
                        let now = std::time::Instant::now();
                        let expired =  ongoings
                            .iter()
                            .filter(|(_, hdlr)| hdlr.sent_at + timeout < now)
                            .map(|(id, _)| *id)
                            .collect::<Vec<_>>();

                        for exp in expired {
                            let hdlr = ongoings.remove(&exp).unwrap();
                            let _ = hdlr.tx.send(RpcResult::Error(RpcError::TimeOut));
                        }
                    },
                    Some(event) = ev_rx.recv() => {
                        match event {
                            RpcEvent::Shutdown => {
                                break;
                            }
                            RpcEvent::OnRequest(request) => {
                                let hdlr = RpcHandler {
                                    sent_at: std::time::Instant::now(),
                                    tx: request.tx
                                };

                                ongoings.insert(request.id, hdlr);
                                let _ = out_tx.send(request.call);
                            },
                            RpcEvent::OnReply(reply) => {
                                if let Some(hdlr) = ongoings.remove(&reply.id) {
                                    let _ = hdlr.tx.send(reply.result);
                                }
                            },
                            RpcEvent::OnAck(ack) => {
                                if let Some(hdlr) = ongoings.get_mut(&ack.id) {
                                    hdlr.sent_at = std::time::Instant::now();
                                }
                            }
                        }
                    }
                }
            }

            // shutdown
            ongoings.into_iter().for_each(|(_, hdlr)| {
                let _ = hdlr.tx.send(RpcResult::Error(RpcError::Shutdown));
            })
        });

        let id = IdGenerator::default();

        let inner = Arc::new(RpcClientInner(cmd_tx.clone()));

        RpcClient {
            tx: cmd_tx,
            id: Arc::new(id),
            inner
        }
    }
}

struct RpcClientInner(mpsc::UnboundedSender<RpcCommand>);

impl Drop for RpcClientInner {
    fn drop(&mut self) {
        use RpcCommand::Shutdown;

        self.0.send(Shutdown);
    }
}

#[derive(Clone)]
pub struct RpcClient {
    tx: mpsc::UnboundedSender<RpcCommand>,
    id: Arc<IdGenerator>,
    // used to stop the actor if the last client has been dropped
    inner: Arc<RpcClientInner>
}

#[derive(TypedBuilder)]
pub struct RpcCallArgs {
    #[builder(setter(transform = |x: impl ToString| x.to_string()))]
    name: String,
    #[builder(setter(transform = |x: impl Serialize| serde_json::to_value(x).unwrap()))]
    args: serde_json::Value,
    #[builder(default, setter(strip_option))]
    source: Option<PeerId>,
    #[builder(default, setter(strip_option))]
    destination: Option<PeerId>
}

impl RpcCallArgs {
    #[inline]
    pub fn call<R: DeserializeOwned>(self, client: &RpcClient) 
    -> impl Future<Output=Result<R, RpcError>> 
    {
        client.call::<R>(self)
    }
}

impl RpcClient {
    pub async fn invoke<Rpc: RemoteProcedureCall>(
        &self, 
        args: impl Into<Rpc::Args>, 
        destinations: impl IntoIterator<Item=PeerId>
    ) -> Result<Rpc::Return, RpcError> {
        RpcCallArgs::builder()
            .name(Rpc::NAME)
            .args(args.into())
            .destination(destinations.into_iter().next().unwrap())
            .build()
            .call::<Rpc::Return>(&self)
            .await
    }

    pub async fn call<R: DeserializeOwned>(&self, args: RpcCallArgs) -> Result<R, RpcError> {
        use RpcCommand::Execute;

        let id = RpcCallId(self.id.next_id());
        let call = RpcCall {
            id,
            name: args.name,
            args: args.args,
            destination: args.destination,
            source: args.source
        };

        let (tx, rx) = oneshot::channel::<RpcResult>();
        
        let request = RpcRequest {
            id,
            call,
            tx,
        };
        
        let _ = self.tx.send(Execute(request));

        let res = rx.await.unwrap();
        match res {
            RpcResult::Ok(value) => Ok(serde_json::from_value(value).unwrap()),
            RpcResult::Error(rpc_error) => Err(rpc_error),
        }       
    }
}