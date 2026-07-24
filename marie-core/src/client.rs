use typed_builder::TypedBuilder;

use crate::{
    di::{self, Container, Resolve}, 
    expert::ExpertClient, 
    model::ModelClient, 
    rpc::RpcClient, 
    secret::{KeyEpoch, SecretKey, SecretManager}, 
    session::client::SessionClient, 
    state_graph::client::StateGraphClient, 
    tools::client::ToolClient, 
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
    container: Container
}

impl Client {
    pub fn new(args: ClientArgs) -> anyhow::Result<Self> {
        let swarm = create_swarm(NodeKind::Client)?;
        let local_peer_id = *swarm.local_peer_id();

        let network = Actor::create(swarm, NodeKind::Client);
        let secret = SecretManager::with_epochs(args.epochs, args.current_epoch)?;

        let container = di::Container::default();
        container.register(secret);
        container.register(LocalPeerId(local_peer_id));
        container.register(rpc::build_client(&network));
        container.register(bootstrap::build_client(
            &network, 
            BootstrapArgs::builder()
                .local_peer_id(local_peer_id)
                .build()
        ));

        // Fail fast at runtime if the di is not properly configured.
        let sessions: SessionClient = container.resolve();
        let workspaces: WorkspaceClient = container.resolve();
        let models: ModelClient = container.resolve();
        let experts: ExpertClient = container.resolve();
        let tools: ToolClient = container.resolve();

        Ok(Client { network, container })
    }

    #[inline]
    pub fn sessions(&self) -> SessionClient {
        self.container.resolve()
    }

    #[inline]
    pub fn workspaces(&self) -> WorkspaceClient {
        self.container.resolve()
    }

    #[inline]
    pub fn models(&self) -> ModelClient {
        self.container.resolve()
    }

    #[inline]
    pub fn tools(&self) -> ToolClient {
        self.container.resolve()
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
    pub fn network(&self) -> SwarmNetwork {
        self.network.clone()
    }
}