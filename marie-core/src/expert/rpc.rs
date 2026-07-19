use std::{borrow::Borrow, sync::Arc};

use async_trait::async_trait;
use libp2p::PeerId;
use parking_lot::Mutex;

use crate::{
    expert::{Expert, ExpertId, catalog::ExpertCatalog},
    rpc::{RemoteProcedureCall, Void},
};

/// Récupère la déclaration d'un expert du catalogue, ou `None` si inconnu de
/// ce nœud — voir [`crate::expert::client::ExpertClient::get`].
#[derive(Clone)]
pub struct GetExpert(pub(crate) Arc<Mutex<ExpertCatalog>>);

#[async_trait]
impl RemoteProcedureCall for GetExpert {
    const NAME: &'static str = "/marie/experts/get";

    type Args = ExpertId;
    type Return = Option<Expert>;

    async fn execute(self, id: ExpertId, _: PeerId) -> Option<Expert> {
        self.0.lock().get(id.borrow())
    }
}

/// Liste tout le catalogue d'experts connu de ce nœud.
#[derive(Clone)]
pub struct ListExpert(pub(crate) Arc<Mutex<ExpertCatalog>>);

#[async_trait]
impl RemoteProcedureCall for ListExpert {
    const NAME: &'static str = "/marie/experts/list";

    type Args = Void;
    type Return = Vec<Expert>;

    async fn execute(self, _: Void, _: PeerId) -> Vec<Expert> {
        self.0.lock().list()
    }
}

/// Crée ou remplace un expert dans le catalogue.
#[derive(Clone)]
pub struct InsertExpert(pub(crate) Arc<Mutex<ExpertCatalog>>);

#[async_trait]
impl RemoteProcedureCall for InsertExpert {
    const NAME: &'static str = "/marie/experts/insert";

    type Args = Expert;
    type Return = Void;

    async fn execute(self, expert: Expert, _: PeerId) -> Void {
        self.0.lock().insert(expert);
        Void
    }
}

/// Met à jour la déclaration d'un expert existant.
#[derive(Clone)]
pub struct UpdateExpert(pub(crate) Arc<Mutex<ExpertCatalog>>);

#[async_trait]
impl RemoteProcedureCall for UpdateExpert {
    const NAME: &'static str = "/marie/experts/update";

    type Args = Expert;
    type Return = Void;

    async fn execute(self, expert: Expert, _: PeerId) -> Void {
        self.0.lock().insert(expert);
        Void
    }
}

/// Retire un expert du catalogue.
#[derive(Clone)]
pub struct RemoveExpert(pub(crate) Arc<Mutex<ExpertCatalog>>);

#[async_trait]
impl RemoteProcedureCall for RemoveExpert {
    const NAME: &'static str = "/marie/experts/remove";

    type Args = ExpertId;
    type Return = Void;

    async fn execute(self, id: ExpertId, _: PeerId) -> Void {
        self.0.lock().remove(id.borrow());
        Void
    }
}
