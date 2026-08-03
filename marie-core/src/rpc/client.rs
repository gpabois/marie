use std::{collections::HashMap, sync::Arc};

use chrono::Utc;
use futures::StreamExt;
use parking_lot::Mutex;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::{select, sync::{oneshot, watch}};
use typed_builder::TypedBuilder;

use crate::{
    di::{Constructible, Resolve}, id::IdGenerator, node::NodeId, post::PostOffice, rpc::{RemoteProcedureCall, RpcError, protocol::RpcId}
};

use super::protocol::*;

#[derive(TypedBuilder)]
pub struct RpcClientArgs {
    id: IdGenerator,
    postoff: PostOffice
}

#[derive(Clone)]
pub struct RpcClient {
    postoff: PostOffice,
    id: IdGenerator,
    tracked: Arc<Mutex<HashMap<RpcId, RpcTracker>>>,
    /// Signale que [`Self::run`] s'est effectivement abonné au flux du
    /// `PostOffice` — attendre ce signal (voir [`Self::wait_for`]) avant
    /// d'émettre un appel évite de perdre silencieusement sa réponse (le
    /// routeur local ne bufferise rien, voir `post::LocalPostMessageRouter`).
    ready: watch::Receiver<bool>
}

impl<C> Constructible<C> for RpcClient 
    where C: Resolve<PostOffice> + Resolve<IdGenerator>
{
    fn construct(container: &C, _: ()) -> Self {
        let args = RpcClientArgs::builder()
            .postoff(container.resolve(()))
            .id(container.resolve(()))
            .build();

        Self::new(args)
    }
}

impl RpcClient {
    pub fn new(
        args: RpcClientArgs
    ) -> Self {
        let (ready_tx, ready_rx) = watch::channel(false);

        let client = RpcClient {
            postoff: args.postoff,
            id: args.id,
            tracked: Arc::new(Mutex::new(HashMap::default())),
            ready: ready_rx
        };

        tokio::spawn(client.clone().run(ready_tx));

        client
    }

    async fn run(self, ready: watch::Sender<bool>) {
        use RpcMessage::{Reply, Ack};
        let mut rx = self.postoff.stream_messages::<RpcMessage>();
        let _ = ready.send(true);

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

    /// Attend que la boucle [`Self::run`] se soit effectivement abonnée au
    /// flux du `PostOffice` — à appeler avant tout appel dans un contexte
    /// (ex. tests avec `PostOffice::local`) où `run` vient d'être spawnée et
    /// n'a pas forcément eu la main : une réponse émise avant cet
    /// abonnement serait sinon silencieusement perdue.
    pub async fn wait_for(&self) {
        let mut ready = self.ready.clone();

        if *ready.borrow() {
            return;
        }

        let _ = ready.wait_for(|ready| *ready).await;
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
        destinations: impl IntoIterator<Item=NodeId>
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
        let id = self.id.next();

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
