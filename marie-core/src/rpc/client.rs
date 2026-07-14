use std::{collections::HashMap, time::Duration};

use futures::{Sink, SinkExt as _, Stream, StreamExt};
use serde::{Serialize, de::DeserializeOwned};
use tokio::{select, sync::{mpsc, oneshot}, task::JoinHandle};

use crate::{id::IdGenerator, rpc::{RpcCall, RpcCallId, RpcError, RpcReply, RpcResult}};

pub struct RpcHandler {
    sent_at: std::time::Instant,
    tx: oneshot::Sender<RpcResult>
}


pub struct RpcClient {
    id: IdGenerator,
    ev_tx: mpsc::UnboundedSender<RpcEvent>,
    incoming_task: JoinHandle<()>,
    outcoming_task: JoinHandle<()>,
}


pub struct RpcRequest {
    id: RpcCallId,
    call: RpcCall,
    tx: oneshot::Sender<RpcResult>
}


enum RpcEvent {
    OnReply(RpcReply),
    OnRequest(RpcRequest), 
    Shutdown
}

impl RpcClient {
    pub fn new<I, O>(outcoming: O, incoming: I) -> Self 
        where O: Sink<RpcCall> + Send + 'static,
              I: Stream<Item=RpcReply> + Send + 'static
    {
        let mut outcoming = Box::pin(outcoming);
        let mut incoming = Box::pin(incoming);

        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<RpcCall>();
        let (ev_tx, mut ev_rx) = mpsc::unbounded_channel::<RpcEvent>();

        let ev_tx_1 = ev_tx.clone();
        let outcoming_task = tokio::spawn(async move {
            loop {
                select! {
                    Some(call) = out_rx.recv() => {
                        let id = call.id;
                        match outcoming.send(call).await {
                            Err(_) => {
                                let reply = RpcReply {
                                    id,
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
                    Some(reply) = incoming.next() => {
                        let _ = ev_tx_2.send(RpcEvent::OnReply(reply));
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

        Self {
            id: IdGenerator::default(),
            ev_tx,
            outcoming_task,
            incoming_task,
        }
    }
}

impl Drop for RpcClient {
    fn drop(&mut self) {
        let _ = self.ev_tx.send(RpcEvent::Shutdown);
        self.outcoming_task.abort();
        self.incoming_task.abort();
    }
}


impl RpcClient {
    pub async fn call_target<R: DeserializeOwned>(&mut self, name: impl ToString, args: impl Serialize, target: impl Serialize) 
        -> Result<R, RpcError>
    {
        let id = RpcCallId(self.id.next_id());
        let call = RpcCall {
            id,
            name: name.to_string(),
            args: serde_json::to_value(args).unwrap(),
            target: Some(serde_json::to_value(target).unwrap())
        };

        let (tx, rx) = oneshot::channel::<RpcResult>();
        
        let request = RpcRequest {
            id,
            call,
            tx,
        };
        
        let _ = self.ev_tx.send(RpcEvent::OnRequest(request));

        let res = rx.await.unwrap();
        match res {
            RpcResult::Ok(value) => Ok(serde_json::from_value(value).unwrap()),
            RpcResult::Error(rpc_error) => Err(rpc_error),
        }
    }
    pub async fn call<R: DeserializeOwned>(&mut self, name: impl ToString, args: impl Serialize) 
        -> Result<R, RpcError>
    {
        let id = RpcCallId(self.id.next_id());
        let call = RpcCall {
            id,
            name: name.to_string(),
            args: serde_json::to_value(args).unwrap(),
            target: None
        };

        let (tx, rx) = oneshot::channel::<RpcResult>();
        
        let request = RpcRequest {
            id,
            call,
            tx,
        };
        
        let _ = self.ev_tx.send(RpcEvent::OnRequest(request));

        let res = rx.await.unwrap();
        match res {
            RpcResult::Ok(value) => Ok(serde_json::from_value(value).unwrap()),
            RpcResult::Error(rpc_error) => Err(rpc_error),
        }
    }
}