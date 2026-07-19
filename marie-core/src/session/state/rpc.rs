use std::sync::Arc;

use async_trait::async_trait;
use libp2p::PeerId;
use parking_lot::Mutex;

use crate::{
    rpc::{RemoteProcedureCall, Void},
    session::state::{catalog::StateGraphCatalog, declaration::{StateGraphDeclaration, StateGraphId}},
};

/// Récupère une déclaration du catalogue de graphes d'états, ou `None` si
/// inconnue de ce nœud — voir
/// [`crate::session::state::client::StateGraphClient::get`].
#[derive(Clone)]
pub struct GetStateGraph(pub(crate) Arc<Mutex<StateGraphCatalog>>);

#[async_trait]
impl RemoteProcedureCall for GetStateGraph {
    const NAME: &'static str = "/marie/state-graphs/get";

    type Args = StateGraphId;
    type Return = Option<StateGraphDeclaration>;

    async fn execute(self, id: StateGraphId, _: PeerId) -> Option<StateGraphDeclaration> {
        self.0.lock().get(&id.to_string())
    }
}

/// Liste tout le catalogue de graphes d'états connu de ce nœud.
#[derive(Clone)]
pub struct ListStateGraph(pub(crate) Arc<Mutex<StateGraphCatalog>>);

#[async_trait]
impl RemoteProcedureCall for ListStateGraph {
    const NAME: &'static str = "/marie/state-graphs/list";

    type Args = Void;
    type Return = Vec<StateGraphDeclaration>;

    async fn execute(self, _: Void, _: PeerId) -> Vec<StateGraphDeclaration> {
        self.0.lock().list()
    }
}

/// Charge utile de [`InsertStateGraph`]/[`UpdateStateGraph`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SetStateGraphRequest {
    pub id: StateGraphId,
    pub declaration: StateGraphDeclaration,
}

/// Crée ou remplace une déclaration dans le catalogue.
#[derive(Clone)]
pub struct InsertStateGraph(pub(crate) Arc<Mutex<StateGraphCatalog>>);

#[async_trait]
impl RemoteProcedureCall for InsertStateGraph {
    const NAME: &'static str = "/marie/state-graphs/insert";

    type Args = SetStateGraphRequest;
    type Return = Void;

    async fn execute(self, request: SetStateGraphRequest, _: PeerId) -> Void {
        self.0.lock().insert(request.id, request.declaration);
        Void
    }
}

/// Met à jour une déclaration existante — même effet qu'[`InsertStateGraph`]
/// (remplacement complet, pas de fusion de delta — une déclaration de graphe
/// n'a pas de champ nécessitant un chiffrement partiel comme
/// `ModelChangeSet`).
#[derive(Clone)]
pub struct UpdateStateGraph(pub(crate) Arc<Mutex<StateGraphCatalog>>);

#[async_trait]
impl RemoteProcedureCall for UpdateStateGraph {
    const NAME: &'static str = "/marie/state-graphs/update";

    type Args = SetStateGraphRequest;
    type Return = Void;

    async fn execute(self, request: SetStateGraphRequest, _: PeerId) -> Void {
        self.0.lock().insert(request.id, request.declaration);
        Void
    }
}

/// Retire une déclaration du catalogue.
#[derive(Clone)]
pub struct RemoveStateGraph(pub(crate) Arc<Mutex<StateGraphCatalog>>);

#[async_trait]
impl RemoteProcedureCall for RemoveStateGraph {
    const NAME: &'static str = "/marie/state-graphs/remove";

    type Args = StateGraphId;
    type Return = Void;

    async fn execute(self, id: StateGraphId, _: PeerId) -> Void {
        self.0.lock().remove(&id.to_string());
        Void
    }
}
