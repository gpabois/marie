use typed_builder::TypedBuilder;

use crate::{
    expert::client::ExpertClient, 
    model::client::ModelClient, network::{actor::{Network, NetworkActor}, 
    bootstrap::{self, client::BootstrapArgs}, create_swarm, peer::NodeKind}, 
    rpc, secret::{KeyEpoch, SecretKey, SecretManager},
    session::client::SessionClient,
    state_graph::client::StateGraphClient,
    workspace::client::WorkspaceClient
};

#[derive(TypedBuilder)]
pub struct ClientArgs {
    epochs: Vec<(KeyEpoch, SecretKey)>,
    current_epoch: KeyEpoch
}

#[derive(Clone)]
pub struct Client {
    network: Network,
    pub sessions: SessionClient,
    pub workspaces: WorkspaceClient,
    pub models: ModelClient,
    pub experts: ExpertClient,
    pub graphs: StateGraphClient,
}

impl Client {
    pub fn new(args: ClientArgs) -> anyhow::Result<Self> {
        let swarm = create_swarm(NodeKind::Client)?;
        let local_peer_id = *swarm.local_peer_id();

        let network = NetworkActor::create(swarm, NodeKind::Client);
        let secret = SecretManager::with_epochs(args.epochs, args.current_epoch)?;

        let rpc = rpc::build_client(&network);
        let bootstrap = bootstrap::build_client(
            &network, 
            BootstrapArgs::builder()
                .local_peer_id(local_peer_id)
                .build()
        );
        
        let sessions = SessionClient::new(
            local_peer_id,
            rpc.clone(),
            bootstrap.clone()
        );

        let workspaces = WorkspaceClient::new(
            local_peer_id,
            rpc.clone(),
            bootstrap.clone()
        );

        let models = ModelClient::new(
            local_peer_id,
            rpc.clone(),
            bootstrap.clone(),
            secret
        );

        let experts = ExpertClient::new(
            local_peer_id,
            rpc.clone(),
            bootstrap.clone()
        );

        let graphs = StateGraphClient::new(
            local_peer_id,
            rpc.clone(),
            bootstrap.clone()
        );

        Ok(Client { network, sessions, workspaces, models, experts, graphs })
    }

    pub async fn connect(&self) -> anyhow::Result<()> {
        self.network.clone().listen(false).await;
        Ok(())
    }

    /// Accès à la couche réseau brute du client — notamment pour construire
    /// un transport `Layer<Send = NetworkCommand, Received = NetworkEvent>`
    /// (voir [`Network::transport`]) ou s'abonner directement à des topics
    /// gossipsub (voir [`Network::subscribe`]), par exemple depuis
    /// `marie_gateway::MarieGatewayActor::create`. `Network` est `Clone` (de
    /// simples `Sender`/`Arc` internes), ce clone est donc peu coûteux.
    pub fn network(&self) -> Network {
        self.network.clone()
    }
}