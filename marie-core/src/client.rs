use typed_builder::TypedBuilder;

use crate::{annuary::capabilities::Capabilities, catalog::store::CatalogStore, di::{Container, Resolve}, events::EventRouter, expert::{Expert, Experts}, graph::Graphs, model::Models, node::OwnNodeId, post::PostMessageRouter, secret::{KeyEpoch, SecretKey, SecretManager, store::SecretStore}, session::{controller::SessionController, store::SessionStore}, tools::Tools};



#[derive(TypedBuilder)]
pub struct ClientArgs {
    epochs: Vec<(KeyEpoch, SecretKey)>,
    current_epoch: KeyEpoch,
}


#[derive(Clone)]
pub struct LocalInMemoryMarie {
    container: Container,
    pub tools: Tools,
    pub graphs: Graphs,
    pub models: Models,
    pub experts: Experts,
    _session_controller: SessionController
}

impl LocalInMemoryMarie {
    pub fn new(args: ClientArgs) -> crate::Result<Self> {
        let secret = SecretManager::with_epochs(args.epochs, args.current_epoch)?;

        let container = Container::default();
        container.register(OwnNodeId::local());
        container.register(secret);
        container.register(SecretStore::in_memory());
        // Postoffice & EventBus configuration
        container.register(PostMessageRouter::new_local());
        container.register(EventRouter::new_local());
        container.register(CatalogStore::in_memory());
        container.register(Capabilities::all());
        container.register(SessionStore::in_memory());

        let tools: Tools = container.resolve(());
        let graphs: Graphs = container.resolve(());
        let models: Models = container.resolve(());
        let experts: Experts = container.resolve(());
        
        let _session_controller: SessionController = container.resolve(());
        

        Ok(Self { 
            tools,
            graphs,
            experts,
            models,
            _session_controller,
            container 
        })
    }

}