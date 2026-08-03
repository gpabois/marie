pub mod client;
#[cfg(feature ="rpc-server")]
mod server;

pub mod protocol;

use async_trait::async_trait;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

#[cfg(feature ="rpc-server")]
pub use server::RpcServer;
pub use client::RpcClient;
pub use marie_macros::core_rpc;

use crate::node::NodeId;
use crate::post::PostError;


#[derive(Clone, Debug, Error, Serialize, Deserialize)]
pub enum RpcError {
    #[error("erreur lors de la désérialization: {0}")]
    DeserializeError(String),
    #[error("erreur lors de l'envoi/réception du message")]
    PostError(#[from] PostError),
    #[error("time-out de l'appel distant")]
    TimeOut,
    #[error("aucun exécuteur n'a été trouvé pour cette procédure {0}")]
    NoExecutorFound(String),
    #[error("arrêt du serveur d'appel distant")]
    Shutdown
}


#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Void;

impl Serialize for Void {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_unit()
    }
}

impl<'de> Deserialize<'de> for Void {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        serde::de::IgnoredAny::deserialize(deserializer)?;
        Ok(Void)
    }
}

#[async_trait]
pub trait RemoteProcedureCall: Sized {
    const NAME: &'static str;
    type Args: Serialize + DeserializeOwned;
    type Return: Serialize + DeserializeOwned;

    #[cfg(feature = "rpc-executor")]
    async fn execute(self, args: Self::Args, caller: NodeId) -> Self::Return;

    #[cfg(feature = "rpc-executor")]
    fn register(self, rpc: &mut RpcServer) where Self: Clone + Send + Sync + 'static {
        let func = move |args, caller| {
            self.clone().execute(args, caller)
        };

        rpc.register(Self::NAME, func);

    }
}

#[cfg(all(test, feature = "rpc-server"))]
mod tests {
    //! Exerce `RpcClient`/`RpcServer` de bout en bout à travers un
    //! [`PostOffice`] local (`LocalPostMessageRouter`, un seul processus,
    //! pas de swarm libp2p) — client et serveur partagent le même
    //! `PostOffice`, comme deux acteurs logiques sur le même nœud.

    use libp2p::identity::Keypair;

    use crate::{
        id::IdGenerator,
        node::NodeId,
        post::PostOffice,
        rpc::{
            RpcError, RpcServer, Void,
            client::{RpcCallArgs, RpcClient, RpcClientArgs},
        },
    };

    fn random_node_id() -> NodeId {
        NodeId::from(Keypair::generate_ed25519().public().to_peer_id())
    }

    fn local_postoffice() -> PostOffice {
        PostOffice::local()
    }

    fn rpc_client(postoff: PostOffice) -> RpcClient {
        RpcClient::new(RpcClientArgs::builder().postoff(postoff).id(IdGenerator::default()).build())
    }

    #[tokio::test]
    async fn call_returns_executor_result() {
        let postoff = local_postoffice();
        let server = RpcServer::new(postoff.clone());
        let client = rpc_client(postoff);

        server.register("echo", |args: String, _caller| async move { args });
        server.wait_for().await;
        client.wait_for().await;

        let result: String = RpcCallArgs::builder()
            .name("echo")
            .args("hello".to_string())
            .destination(random_node_id())
            .build()
            .call(&client)
            .await
            .unwrap();

        assert_eq!(result, "hello");
    }

    #[tokio::test]
    async fn call_fails_when_no_executor_registered() {
        let postoff = local_postoffice();
        let server = RpcServer::new(postoff.clone());
        let client = rpc_client(postoff);

        // Aucun executor enregistré : on attend juste que les deux boucles
        // écoutent, pour que l'appel atteigne bien un `RpcServer` qui
        // répondra `NoExecutorFound` (et que le client reçoive bien cette
        // réponse) plutôt que d'être perdu en route.
        server.wait_for().await;
        client.wait_for().await;

        let err = RpcCallArgs::builder()
            .name("does_not_exist")
            .args(Void)
            .destination(random_node_id())
            .build()
            .call::<Void>(&client)
            .await
            .unwrap_err();

        assert!(matches!(err, RpcError::NoExecutorFound(name) if name == "does_not_exist"));
    }

    #[tokio::test]
    async fn call_fails_when_reply_does_not_match_expected_return_type() {
        let postoff = local_postoffice();
        let server = RpcServer::new(postoff.clone());
        let client = rpc_client(postoff);

        server.register("greet", |_args: Void, _caller| async move { "not-a-number".to_string() });
        server.wait_for().await;
        client.wait_for().await;

        let err = RpcCallArgs::builder()
            .name("greet")
            .args(Void)
            .destination(random_node_id())
            .build()
            .call::<u64>(&client)
            .await
            .unwrap_err();

        assert!(matches!(err, RpcError::DeserializeError(_)));
    }

    #[tokio::test]
    async fn concurrent_calls_are_routed_to_the_right_caller() {
        let postoff = local_postoffice();
        let server = RpcServer::new(postoff.clone());
        let client = rpc_client(postoff);

        server.register("double", |args: u32, _caller| async move { args * 2 });
        server.register("square", |args: u32, _caller| async move { args * args });
        server.wait_for().await;
        client.wait_for().await;

        let double_call = RpcCallArgs::builder()
            .name("double")
            .args(21u32)
            .destination(random_node_id())
            .build()
            .call::<u32>(&client);

        let square_call = RpcCallArgs::builder()
            .name("square")
            .args(6u32)
            .destination(random_node_id())
            .build()
            .call::<u32>(&client);

        let (double_result, square_result) = tokio::join!(double_call, square_call);

        assert_eq!(double_result.unwrap(), 42);
        assert_eq!(square_result.unwrap(), 36);
    }
}