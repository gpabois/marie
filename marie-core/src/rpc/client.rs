use std::{collections::HashMap, sync::Arc};

use chrono::Utc;
use futures::StreamExt;
use libp2p::PeerId;
use parking_lot::Mutex;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::{select, sync::oneshot};
use typed_builder::TypedBuilder;

use crate::{
    di::{Factory, Get}, id::IdGenerator, node::NodeId, post::PostOffice, rpc::{RemoteProcedureCall, RpcError, protocol::RpcId}
};

use super::protocol::*;

#[derive(Clone)]
pub struct RpcClient {
    postoff: PostOffice,
    id: Arc<IdGenerator>,
    tracked: Arc<Mutex<HashMap<RpcId, RpcTracker>>>
}

impl RpcClient {
    pub fn new(
        postoff: PostOffice
    ) -> Self {
        let client = RpcClient {
            postoff, 
            id: Arc::new(IdGenerator::default()),
            tracked: Arc::new(Mutex::new(HashMap::default()))
        };

        tokio::spawn(client.clone().run());

        client
    }

    async fn run(self) {
        use RpcMessage::{Reply, Ack};
        let mut rx = self.postoff.stream_messages::<RpcMessage>();
        loop {
            select! {
                Some(msg) = rx.next() => {
                    match msg.payload {
                        Reply(reply) => self.handle_reply(reply),
                        Ack(ack) => self.handle_ack(ack),
                        _ => {}
                    }
                }
            }
        }
    }

    fn handle_ack(&self, ack: RpcAck) {
        if let Some(tracker) = self.tracked.lock().get_mut(&ack.id) {
            // impl. ack
        }
    }

    fn handle_reply(&self, reply: RpcReply) {
        if let Some(tracker) = self.tracked.lock().remove(&reply.id) {
            let _ = tracker.tx.send(reply.result);
        }
    }

    fn send(&self, msg: impl Into<RpcMessage>, destination: NodeId) -> Result<(), RpcError> {
        self.postoff.send(msg, destination)?;
        Ok(())
    }

    fn track(&self, id: RpcId, tracker: RpcTracker) {
        self.tracked.lock().insert(id, tracker);
    }
}

impl<C> Factory<C> for RpcClient where C: Get<PostOffice> {
    fn create(container: &C) -> Self {
        let postoff: PostOffice = container.get();
        Self::new(postoff)
    }
}

#[derive(TypedBuilder)]
pub struct RpcCallArgs {
    #[builder(setter(transform = |x: impl ToString| x.to_string()))]
    name: String,
    #[builder(setter(transform = |x: impl Serialize| serde_json::to_value(x).unwrap()))]
    args: serde_json::Value,
    #[builder(setter(transform = |x: impl Into<NodeId>| x.into()))]
    destination: NodeId
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
        let id = RpcId(self.id.next_id());
        let call = RpcCall {
            id,
            name: args.name,
            args: args.args,
        };

        let (tx, rx) = oneshot::channel::<Result<Value, RpcError>>();
        
        let tracker = RpcTracker {
            sent_at: Utc::now(),
            tx
        };

        self.track(id, tracker);
        self.send(call, args.destination)?;

        let ret = rx.await.unwrap().and_then(|value| serde_json::from_value::<R>(value).map_err(|err| RpcError::DeserializeError(err.to_string())))?;
        Ok(ret)
    }
}



struct RpcTracker {
    sent_at: chrono::DateTime<Utc>,
    tx: oneshot::Sender<Result<Value, RpcError>>
}
