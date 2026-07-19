use std::sync::Arc;

use libp2p::PeerId;
use serde::{Serialize, de::DeserializeOwned};

use crate::rpc::{RpcClient, RpcError, RpcServer, client::RpcCallArgs};

/// Décrit un point RPC unique : son nom sur le réseau, le type de ses
/// arguments, celui de son retour, et l'exécuteur qui les relie.
///
/// Sans ce trait, ces quatre éléments sont dispersés pour chaque opération :
/// une constante `RPC_*` dans `mod.rs`, une closure d'exécution enregistrée
/// dans `server.rs`, et un appel `RpcCallArgs::builder()` répétant le même
/// nom dans `client.rs` — rien n'empêche que le nom passé côté client diverge
/// silencieusement de celui enregistré côté serveur (mauvais import de
/// constante, faute de frappe). En implémentant `Rpc` sur un type par
/// opération, le nom et les types ne peuvent être définis qu'une fois ; voir
/// [`RpcServer::register_rpc`] et [`RpcClient::invoke`].
pub trait Rpc: Send + Sync + 'static {
    /// Nom de l'appel sur le réseau (ancien `RPC_*`).
    const NAME: &'static str;

    type Args: Serialize + DeserializeOwned + Send + 'static;
    type Return: Serialize + DeserializeOwned + Send + 'static;

    /// Logique d'exécution côté serveur. Reçoit `&self` plutôt qu'un état
    /// capturé par closure : le type implémentant `Rpc` porte lui-même l'état
    /// nécessaire (ex: `Arc<Mutex<Catalog>>`), ce qui le rend nommable et
    /// réutilisable côté client comme paramètre de type pour
    /// [`RpcClient::invoke`], sans qu'une instance y soit construite.
    fn execute(&self, args: Self::Args, source: PeerId) -> impl Future<Output = Self::Return> + Send;
}

impl RpcServer {
    /// Enregistre un exécuteur typé (voir [`Rpc`]) — équivalent à
    /// `self.register(R::NAME, ...)` mais sans exposer le nom, le type des
    /// arguments ou celui du retour au call-site : ils sont fixés une bonne
    /// fois pour toutes par l'implémentation de `R`.
    pub fn register_rpc<R: Rpc>(&mut self, rpc: R) {
        let rpc = Arc::new(rpc);
        self.register(R::NAME, move |args: R::Args, source: PeerId| {
            let rpc = rpc.clone();
            async move { rpc.execute(args, source).await }
        });
    }
}

impl RpcClient {
    /// Appelle un RPC typé (voir [`Rpc`]) — équivalent à construire un
    /// `RpcCallArgs::builder().name(R::NAME)...` mais le nom et les types
    /// d'argument/retour sont déduits de `R` plutôt que répétés au call-site.
    pub async fn invoke<R: Rpc>(&self, args: R::Args, destination: PeerId) -> Result<R::Return, RpcError> {
        RpcCallArgs::builder()
            .name(R::NAME)
            .args(args)
            .destination(destination)
            .build()
            .call::<R::Return>(self)
            .await
    }
}
