use typed_builder::TypedBuilder;

use crate::{
    annuary::{AnnuaryEventRouter, LocalAnnuaryEventRouter, capabilities::Capabilities},
    catalog::store::CatalogStore,
    di::{Container, Resolve},
    events::EventRouter,
    expert::Experts,
    graph::Graphs,
    hitl::store::HitlStore,
    model::Models,
    node::OwnNodeId,
    post::PostMessageRouter,
    secret::{KeyEpoch, SecretKey, SecretManager, store::SecretStore},
    session::{client::SessionClientFactory, controller::SessionController, frames::store::SessionFrameStore, logs::store::SessionLogStore, store::SessionStore},
    store::PgStore,
    tools::Tools
};

#[derive(TypedBuilder)]
pub struct LocalMarieArgs {
    epochs: Vec<(KeyEpoch, SecretKey)>,
    current_epoch: KeyEpoch,
    database_mode: DatabaseMode
}

#[derive(Default)]
pub enum DatabaseMode {
    Postgres(String),
    #[default]
    InMemory
}

#[derive(Clone)]
pub struct LocalMarie {
    container: Container,
    pub tools: Tools,
    pub graphs: Graphs,
    pub models: Models,
    pub experts: Experts,
    pub sessions: SessionController,
    pub session_client_factory: SessionClientFactory
}

impl LocalMarie {
    pub async fn new(args: LocalMarieArgs) -> crate::Result<Self> {
        let secret = SecretManager::with_epochs(args.epochs, args.current_epoch)?;

        let container = Container::default();
        let own_node_id = OwnNodeId::local();
        container.register(own_node_id.clone());
        container.register(secret);
        container.register(SecretStore::in_memory());
        // Postoffice & EventBus configuration
        container.register(PostMessageRouter::new_local());
        container.register(EventRouter::new_local());
        container.register(Capabilities::all());
        // Annuaire de pairs — purement local (aucun réseau), voir
        // `LocalAnnuaryEventRouter`. Requis pour résoudre `Annuary`
        // (voir `SessionClientFactory`), qui n'a jamais été un pré-requis de
        // `LocalMarie` avant l'ajout de cette dernière.
        container.register(AnnuaryEventRouter::new(LocalAnnuaryEventRouter::new(own_node_id)));

        match args.database_mode {
            DatabaseMode::Postgres(url) => {
                let pg = PgStore::connect(&url).await?;

                container.register(CatalogStore::new(pg.clone()));
                container.register(SessionStore::new(pg.clone()));
                container.register(SessionFrameStore::new(pg.clone()));
                container.register(SessionLogStore::new(pg.clone()));
                container.register(HitlStore::new(pg.clone()));
            }
            DatabaseMode::InMemory => {
                container.register(CatalogStore::in_memory());
                container.register(SessionStore::in_memory());
                container.register(SessionFrameStore::in_memory());
                container.register(SessionLogStore::in_memory());
                container.register(HitlStore::in_memory());
            }
        }


        let tools: Tools = container.resolve(());
        let graphs: Graphs = container.resolve(());
        let models: Models = container.resolve(());
        let experts: Experts = container.resolve(());

        let sessions: SessionController = container.resolve(());
        let session_client_factory: SessionClientFactory = container.resolve(());

        Ok(Self {
            tools,
            graphs,
            experts,
            models,
            sessions,
            session_client_factory,
            container
        })
    }

}