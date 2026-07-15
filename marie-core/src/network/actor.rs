use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use anyhow::bail;
use futures::sink::Sink;
use futures::{Stream, StreamExt as _, future::BoxFuture};
use libp2p::{Multiaddr, PeerId, gossipsub, identify, mdns, request_response::{self, OutboundRequestId, ResponseChannel}, swarm::SwarmEvent};
use serde::{Serialize, de::DeserializeOwned};
use tokio::{select, sync::{broadcast, mpsc, oneshot}};
use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};
use tracing::{warn, info};

use crate::layer::Layer;
use crate::network::Frame;
use crate::network::actor::NetworkCommand::SendFrame;
use crate::{
    expert::{catalog::ExpertId, declaration::ExpertDeclaration},
    job::Job,
    mode::state_graph::{catalog::StateGraphId, declaration::StateGraphDeclaration},
    model::declaration::{EncryptedModel, Model, ModelId},
    network::{
        MarieSwarm,
        cp::{
            self,
            rpc::{RpcCall, SetExpertRequest, SetModelRequest, SetStateGraphRequest, SetToolRequest},
        },
    },
    secret::{EncryptedSecret, SecretManager, SecretResult},
    tools::{catalog::ToolId, declaration::ToolDeclaration},
};

/// Handler d'une RPC enregistrée dynamiquement (voir [`NetworkClient::register_rpc`]).
pub type RpcHandler = Arc<dyn Fn(serde_json::Value) -> BoxFuture<'static, Result<serde_json::Value, anyhow::Error>> + Send + Sync>;

/// Appel RPC en vol, en attente de réponse.
struct PendingRpc {
    tx: oneshot::Sender<cp::rpc::RpcResult>,
    call: cp::rpc::RpcCall,
    /// `true` si la cible a été résolue implicitement via `cp_peer_id` plutôt
    /// qu'explicitement demandée par l'appelant (`rpc_to`) — dans ce cas, un
    /// échec de livraison peut être retenté vers un autre control plane connu
    /// (voir le traitement de `OutboundFailure` dans `NetworkActor::run`).
    retry_via_cp_failover: bool,
}

pub enum NetworkCommand {
    SendFrame(Frame),
    Subscribe(gossipsub::IdentTopic),
    Publish {
        topic: gossipsub::IdentTopic,
        payload: Vec<u8>
    },
    RemoteProcedureCall {
        tx: oneshot::Sender<cp::rpc::RpcResult>,
        call: cp::rpc::RpcCall,
        to: Option<PeerId>
    },
    RemoteProcedureReply {
        channel: ResponseChannel<cp::rpc::RpcResult>,
        result: cp::rpc::RpcResult
    },
    /// Interroge l'état de connexion libp2p d'un pair — utilisé par le control
    /// plane pour le healthchecking (pas de RPC applicatif requis côté cible).
    IsConnected {
        peer_id: PeerId,
        tx: oneshot::Sender<bool>,
    },
    /// Enregistre un handler local pour les appels RPC entrants portant ce
    /// nom — voir [`NetworkClient::register_rpc`].
    RegisterHandler {
        name: String,
        handler: RpcHandler,
    },
    /// S'abonne à un topic gossipsub (`node_gossip`) — voir
    /// [`NetworkClient::subscribe_gossip`].
    SubscribeGossip {
        topic: gossipsub::IdentTopic,
    },
    /// Publie sur un topic gossipsub (`node_gossip`) — voir
    /// [`NetworkClient::publish_gossip`].
    PublishGossip {
        topic: gossipsub::IdentTopic,
        data: Vec<u8>,
    },
    /// Arrête [`NetworkActor::run`] proprement — voir
    /// [`NetworkClient::shutdown`]. Sans ça, la tâche portant le `Swarm`
    /// libp2p ne s'arrête jamais d'elle-même (voir `run`, une boucle
    /// infinie) : même un rôle qui termine sa propre boucle proprement (voir
    /// `network::cp::start_control_plane` et consorts) la laisserait tourner
    /// en arrière-plan, connexions ouvertes, indéfiniment.
    Shutdown,
}

/// Jeton de réponse d'une exécution RPC entrante : `Arc<Mutex<Option<...>>>`
/// plutôt qu'un `oneshot::Sender` nu, parce que [`NetworkEvent`] doit être
/// `Clone` pour être diffusé à plusieurs abonnés indépendants (voir
/// [`NetworkEventHandler`]) — un `oneshot::Sender` ne l'est pas. Plusieurs
/// abonnés peuvent donc voir le même `RequestRemoteProcedureExecution`, mais
/// un seul (le dispatcher RPC du rôle courant, voir `execute_rpc` côté
/// worker/control plane) doit effectivement y répondre : le premier à
/// réussir `.take()` gagne, les autres voient `None` et l'ignorent.
type RpcReplySlot = Arc<Mutex<Option<oneshot::Sender<cp::rpc::RpcResult>>>>;

#[derive(Clone)]
pub enum NetworkEvent {
    ReceivedFrame(Frame),
    RequestRemoteProcedureExecution {
        tx: RpcReplySlot,
        call: cp::rpc::RpcCall,
        /// Le pair qui a émis l'appel — authentifié par la connexion libp2p
        /// sous-jacente (Noise), donc impossible à usurper.
        peer: PeerId,
    },
    /// Un pair identifié via libp2p se déclare `ControlPlane` — candidat à rejoindre
    /// le cluster Raft du control plane.
    ControlPlanePeerDiscovered {
        peer_id: PeerId,
        addr: Option<Multiaddr>,
    },
    /// Un pair identifié via libp2p se déclare `Worker` (ou `WorkerOrchestrator`) —
    /// candidat à l'enregistrement dans le registre des workers du control plane.
    WorkerPeerDiscovered {
        peer_id: PeerId,
        addr: Option<Multiaddr>,
    },
    /// Un pair identifié via libp2p se déclare `Persistency` — candidat à
    /// l'enregistrement dans `ControlPlaneState::persistency_nodes` (voir
    /// `network::persistency`).
    PersistencyPeerDiscovered {
        peer_id: PeerId,
        addr: Option<Multiaddr>,
    },
    /// Un pair (quel qu'il soit) vient de perdre sa dernière connexion — plus
    /// aucune connexion libp2p établie avec lui. Utilisé pour retirer
    /// automatiquement ses enregistrements RPC dynamiques (voir
    /// `cp::DynamicRpcRegistry`).
    PeerDisconnected {
        peer_id: PeerId,
    },
    /// Message reçu sur un topic gossipsub (`node_gossip`) auquel ce nœud est
    /// abonné — voir [`NetworkClient::subscribe_gossip`].
    PubSubReceived {
        topic: String,
        data: Vec<u8>,
        /// Le pair qui nous a directement transmis ce message (voir
        /// `gossipsub::Event::Message::propagation_source`) — pas
        /// nécessairement son auteur d'origine, mais garanti joignable
        /// (connexion libp2p établie), contrairement à ce dernier. Utilisé
        /// par `network::persistency` pour amorcer une session jamais vue
        /// (voir `RpcCall::FETCH_SESSION`) auprès de qui vient de la gossiper.
        source: PeerId,
    }
}

/// Capacité du canal de diffusion des [`NetworkEvent`] (voir
/// [`NetworkClient::subscribe_events`]). Un abonné qui prend trop de retard
/// perd les événements les plus anciens (voir [`NetworkEventHandler`]) —
/// notamment un `RequestRemoteProcedureExecution`, qui ne sera alors jamais
/// répondu (voir le traitement de `rx.await` dans `NetworkActor::run`).
/// Généreuse pour limiter ce risque en pratique, sans prétendre l'éliminer :
/// un appelant dont la requête est ainsi perdue la retentera de toute façon
/// (voir `FORWARD_RETRY_ATTEMPTS`).
const NETWORK_EVENTS_CAPACITY: usize = 1024;

/// Flux de [`NetworkEvent`] : multi-abonnés (voir
/// [`NetworkClient::subscribe_events`]), contrairement à l'ancien canal
/// mono-consommateur — plusieurs composants indépendants (la boucle
/// applicative du rôle courant, `session::client::SessionClient`,
/// ...) peuvent donc chacun observer le flux complet sans se le disputer.
/// `Lagged` (abonné trop en retard) est absorbé silencieusement : voir
/// [`NETWORK_EVENTS_CAPACITY`] pour les conséquences.
pub struct NetworkReceiver(BroadcastStream<NetworkEvent>);

impl Stream for NetworkReceiver {
    type Item = NetworkEvent;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        loop {
            return match std::pin::Pin::new(&mut self.0).poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(event))) => std::task::Poll::Ready(Some(event)),
                std::task::Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(skipped)))) => {
                    warn!(skipped, "abonné réseau en retard, événements perdus");
                    continue;
                }
                std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
                std::task::Poll::Pending => std::task::Poll::Pending,
            };
        }
    }
}

#[derive(Clone)]
pub struct NetworkSender(mpsc::UnboundedSender<NetworkCommand>);

impl Sink<NetworkCommand> for NetworkSender {
    type Error = anyhow::Error;

    fn poll_ready(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: NetworkCommand) -> Result<(), Self::Error> {
        self.0.send(item)?;
        Ok(())
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_close(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
       std::task::Poll::Ready(Ok(()))
    }
}

pub struct NetworkLayer(NetworkSender, NetworkReceiver);

impl Layer for NetworkLayer {
    type Send = NetworkCommand;
    type Received = NetworkEvent;
    type Sender = NetworkSender;
    type Receiver = NetworkReceiver;

    fn split(self) -> (Self::Sender, Self::Receiver) {
        (self.0, self.1)
    }
}

impl NetworkLayer {
    pub fn split(self) -> (NetworkSender, NetworkReceiver) {
        (self.0, self.1)
    }
}

#[derive(Clone)]
pub struct NetworkService {
    commands: mpsc::UnboundedSender<NetworkCommand>,
    /// Diffusion des [`NetworkEvent`] de ce nœud — voir [`Self::subscribe_events`].
    events: broadcast::Sender<NetworkEvent>,
    /// Identité libp2p de ce nœud — voir [`Self::decrypt_secret`].
    local_peer_id: PeerId,
    /// Secret partagé du cluster (voir `secret::SecretManager`) : permet de
    /// déchiffrer localement tout secret qu'un autre nœud a chiffré à
    /// l'intention de celui-ci (voir [`Self::decrypt_secret`]), sans que la
    /// clé maître elle-même n'ait jamais à transiter sur le réseau.
    secret: Arc<SecretManager>,
}

impl NetworkService {
    /// Récupère la couche de transport du réseau
    pub fn transport(&self) -> NetworkLayer {
        let sender = NetworkSender(self.commands.clone());
        let receiver = NetworkReceiver(BroadcastStream::new(self.events.subscribe()));
        NetworkLayer(sender, receiver)
    }

    /// Send a frame to a peer
    pub fn send_frame(&self, frame: Frame) -> Result<(), anyhow::Error> {
        self.commands.send(SendFrame(frame))?;
        Ok(())
    }

    /// S'abonne au flux de [`NetworkEvent`] de ce nœud. Chaque appel
    /// retourne un [`NetworkEventHandler`] indépendant, démarrant à partir de
    /// maintenant (les événements précédents ne sont pas rejoués) :
    /// plusieurs consommateurs peuvent donc chacun avoir le leur (voir
    /// `session::client::SessionClient`, qui s'intéresse aux
    /// `GossipMessageReceived` et aux `PersistencyPeerDiscovered`, sans se
    /// disputer la consommation avec la boucle applicative qui doit
    /// répondre aux `RequestRemoteProcedureExecution`).
    pub fn subscribe_events(&self) -> NetworkReceiver {
        NetworkReceiver(BroadcastStream::new(self.events.subscribe()))
    }

    /// Récupère la déclaration d'un modèle auprès du control plane. La clé
    /// API voyage chiffrée (voir [`Self::decrypt_secret`]) : le control plane
    /// la chiffre spécifiquement pour ce nœud (voir
    /// `SecretManager::encrypt_api_key`), et elle n'est déchiffrée en clair
    /// qu'ici, localement.
    pub async fn get_model(&self, model_id: impl Into<ModelId>) -> Result<Option<Model>, anyhow::Error> {
        let encrypted: Option<EncryptedModel> =
            self.rpc(RpcCall::new(RpcCall::GET_MODEL, model_id.into())).await?;

        let Some(encrypted) = encrypted else {
            return Ok(None);
        };

        let api_key = self.decrypt_secret(encrypted.api_key())?;
        Ok(Some(encrypted.decrypt(api_key)))
    }

    /// Liste tout le catalogue de modèles connu du control plane. Comme
    /// [`Self::get_model`], chaque clé API voyage chiffrée spécifiquement
    /// pour ce nœud et n'est déchiffrée en clair qu'ici, localement.
    pub async fn list_models(&self) -> Result<HashMap<ModelId, Model>, anyhow::Error> {
        let entries: Vec<(ModelId, EncryptedModel)> = self.rpc(RpcCall::new(RpcCall::LIST_MODELS, ())).await?;

        entries
            .into_iter()
            .map(|(id, encrypted)| {
                let api_key = self.decrypt_secret(encrypted.api_key())?;
                Ok((id, encrypted.decrypt(api_key)))
            })
            .collect()
    }

    /// Crée ou remplace la déclaration d'un modèle dans le catalogue
    /// (répliqué via Raft, voir `ControlPlaneRequest::SetModel`).
    pub async fn set_model(&self, id: impl Into<ModelId>, declaration: Model) -> Result<(), anyhow::Error> {
        let request = SetModelRequest { id: id.into(), declaration };
        self.rpc::<cp::rpc::Void>(RpcCall::new(RpcCall::SET_MODEL, request)).await?;
        Ok(())
    }

    /// Retire un modèle du catalogue (répliqué via Raft, voir
    /// `ControlPlaneRequest::RemoveModel`).
    pub async fn remove_model(&self, id: impl Into<ModelId>) -> Result<(), anyhow::Error> {
        self.rpc::<cp::rpc::Void>(RpcCall::new(RpcCall::REMOVE_MODEL, id.into())).await?;
        Ok(())
    }

    /// Récupère la déclaration d'un tool auprès du control plane, sur le
    /// même modèle que [`Self::get_model`] — sans déchiffrement, une
    /// déclaration de tool ne porte aucun secret (voir
    /// [`crate::tools::declaration::ToolDeclaration`]).
    pub async fn get_tool(&self, id: impl Into<ToolId>) -> Result<Option<ToolDeclaration>, anyhow::Error> {
        self.rpc(RpcCall::new(RpcCall::GET_TOOL, id.into())).await
    }

    /// Liste tout le catalogue de tools connu du control plane.
    pub async fn list_tools(&self) -> Result<HashMap<ToolId, ToolDeclaration>, anyhow::Error> {
        self.rpc(RpcCall::new(RpcCall::LIST_TOOLS, ())).await
    }

    /// Crée ou remplace la déclaration d'un tool dans le catalogue
    /// (répliqué via Raft, voir `ControlPlaneRequest::SetTool`).
    pub async fn set_tool(&self, id: impl Into<ToolId>, declaration: ToolDeclaration) -> Result<(), anyhow::Error> {
        let request = SetToolRequest { id: id.into(), declaration };
        self.rpc::<cp::rpc::Void>(RpcCall::new(RpcCall::SET_TOOL, request)).await?;
        Ok(())
    }

    /// Retire un tool du catalogue (répliqué via Raft, voir
    /// `ControlPlaneRequest::RemoveTool`).
    pub async fn remove_tool(&self, id: impl Into<ToolId>) -> Result<(), anyhow::Error> {
        self.rpc::<cp::rpc::Void>(RpcCall::new(RpcCall::REMOVE_TOOL, id.into())).await?;
        Ok(())
    }

    /// Récupère la déclaration d'un expert auprès du control plane, sur le
    /// même modèle que [`Self::get_tool`] — sans déchiffrement, une
    /// déclaration d'expert ne porte aucun secret (voir
    /// [`crate::expert::declaration::ExpertDeclaration`]).
    pub async fn get_expert(&self, id: impl Into<ExpertId>) -> Result<Option<ExpertDeclaration>, anyhow::Error> {
        self.rpc(RpcCall::new(RpcCall::GET_EXPERT, id.into())).await
    }

    /// Liste tout le catalogue d'experts connu du control plane.
    pub async fn list_experts(&self) -> Result<HashMap<ExpertId, ExpertDeclaration>, anyhow::Error> {
        self.rpc(RpcCall::new(RpcCall::LIST_EXPERTS, ())).await
    }

    /// Crée ou remplace la déclaration d'un expert dans le catalogue
    /// (répliqué via Raft, voir `ControlPlaneRequest::SetExpert`).
    pub async fn set_expert(&self, id: impl Into<ExpertId>, declaration: ExpertDeclaration) -> Result<(), anyhow::Error> {
        let request = SetExpertRequest { id: id.into(), declaration };
        self.rpc::<cp::rpc::Void>(RpcCall::new(RpcCall::SET_EXPERT, request)).await?;
        Ok(())
    }

    /// Retire un expert du catalogue (répliqué via Raft, voir
    /// `ControlPlaneRequest::RemoveExpert`).
    pub async fn remove_expert(&self, id: impl Into<ExpertId>) -> Result<(), anyhow::Error> {
        self.rpc::<cp::rpc::Void>(RpcCall::new(RpcCall::REMOVE_EXPERT, id.into())).await?;
        Ok(())
    }

    /// Récupère la déclaration d'un graphe d'états auprès du control plane,
    /// sur le même modèle que [`Self::get_expert`] — sans déchiffrement, une
    /// déclaration de graphe ne porte aucun secret (voir
    /// [`crate::mode::state_graph::declaration::StateGraphDeclaration`]).
    pub async fn get_state_graph(&self, id: impl Into<StateGraphId>) -> Result<Option<StateGraphDeclaration>, anyhow::Error> {
        self.rpc(RpcCall::new(RpcCall::GET_STATE_GRAPH, id.into())).await
    }

    /// Liste tout le catalogue de graphes d'états connu du control plane.
    pub async fn list_state_graphs(&self) -> Result<HashMap<StateGraphId, StateGraphDeclaration>, anyhow::Error> {
        self.rpc(RpcCall::new(RpcCall::LIST_STATE_GRAPHS, ())).await
    }

    /// Crée ou remplace la déclaration d'un graphe d'états dans le catalogue
    /// (répliqué via Raft, voir `ControlPlaneRequest::SetStateGraph`).
    pub async fn set_state_graph(&self, id: impl Into<StateGraphId>, declaration: StateGraphDeclaration) -> Result<(), anyhow::Error> {
        let request = SetStateGraphRequest { id: id.into(), declaration };
        self.rpc::<cp::rpc::Void>(RpcCall::new(RpcCall::SET_STATE_GRAPH, request)).await?;
        Ok(())
    }

    /// Retire un graphe d'états du catalogue (répliqué via Raft, voir
    /// `ControlPlaneRequest::RemoveStateGraph`).
    pub async fn remove_state_graph(&self, id: impl Into<StateGraphId>) -> Result<(), anyhow::Error> {
        self.rpc::<cp::rpc::Void>(RpcCall::new(RpcCall::REMOVE_STATE_GRAPH, id.into())).await?;
        Ok(())
    }

    /// Déchiffre un secret (ex. clé API d'un modèle, voir
    /// [`Self::get_model`]) chiffré par son expéditeur spécifiquement pour ce
    /// nœud (voir `SecretManager::derive_node_key` / `encrypt_api_key`) — les
    /// deux parties partagent le même secret de cluster, la clé dérivée n'a
    /// donc jamais besoin de transiter sur le réseau.
    pub fn decrypt_secret(&self, encrypted: &EncryptedSecret) -> SecretResult<String> {
        let node_key = self.secret.derive_node_key(&self.local_peer_id)?;
        self.secret.decrypt_api_key(encrypted, &node_key)
    }

    /// Soumet un job au control plane. Le job est proposé au cluster Raft et,
    /// une fois committé, ordonnancé sur un worker disponible.
    pub async fn spawn_job(&self, job: Job) -> Result<(), anyhow::Error> {
        self.rpc::<cp::rpc::Void>(RpcCall::new(RpcCall::SUBMIT_JOB, job)).await?;
        Ok(())
    }

    /// Enregistre `handler` pour exécuter localement tout appel RPC portant
    /// `name`, et annonce ce nom au control plane connu
    /// (`RpcCall::REGISTER_RPC`) pour qu'il y relaie les appels reçus pour ce
    /// nom. Si tous les nœuds l'ayant enregistré se déconnectent, le control
    /// plane retire l'enregistrement automatiquement.
    pub async fn register_rpc<F, Fut>(&self, name: impl ToString, handler: F) -> Result<(), anyhow::Error>
    where
        F: Fn(serde_json::Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<serde_json::Value, anyhow::Error>> + Send + 'static,
    {
        use NetworkCommand::RegisterHandler;

        let name = name.to_string();
        let handler: RpcHandler = Arc::new(move |args| Box::pin(handler(args)));
        let _ = self.commands.send(RegisterHandler { name: name.clone(), handler });

        self.rpc::<cp::rpc::Void>(RpcCall::new(RpcCall::REGISTER_RPC, name)).await?;
        Ok(())
    }

    /// S'abonne à un topic gossipsub (`node_gossip`) : les messages publiés
    /// dessus par d'autres nœuds abonnés remonteront via
    /// `NetworkEvent::GossipMessageReceived`.
    pub fn subscribe(&self, topic: impl Into<String>) {
        use NetworkCommand::SubscribeGossip;
        let _ = self.commands.send(SubscribeGossip { topic: gossipsub::IdentTopic::new(topic) });
    }

    /// Publie `data` (sérialisé en JSON) sur un topic gossipsub (`node_gossip`).
    /// Best-effort : échoue silencieusement si ce nœud n'a pour l'instant
    /// aucun pair du mesh gossipsub pour ce topic (personne à qui l'envoyer).
    pub fn publish(&self, topic: impl Into<String>, data: impl Serialize) -> Result<(), anyhow::Error> {
        use NetworkCommand::PublishGossip;
        let data = serde_json::to_vec(&data)?;
        let _ = self.commands.send(PublishGossip { topic: gossipsub::IdentTopic::new(topic), data });
        Ok(())
    }

    /// Arrête [`NetworkActor::run`] de ce nœud — voir
    /// `NetworkCommand::Shutdown`. À appeler une fois qu'il n'y a plus rien
    /// à envoyer/recevoir sur ce `NetworkClient` (voir
    /// `network::cp::start_control_plane`/`network::worker::start_worker`/
    /// `network::persistency::start_persistency`, qui l'appellent en tout
    /// dernier, après avoir drainé leur propre travail en vol) : les
    /// `NetworkCommand` envoyés après coup sont silencieusement perdus,
    /// l'actor n'étant plus là pour les traiter.
    pub fn shutdown(&self) {
        let _ = self.commands.send(NetworkCommand::Shutdown);
    }

    /// Indique si ce nœud est actuellement connecté à `peer_id` au niveau
    /// libp2p — un signal de vivacité indépendant de tout RPC applicatif.
    pub async fn is_connected(&self, peer_id: PeerId) -> Result<bool, anyhow::Error> {
        use NetworkCommand::IsConnected;

        let (tx, rx) = oneshot::channel();
        let _ = self.commands.send(IsConnected { peer_id, tx });
        Ok(rx.await?)
    }

    pub async fn rpc_to<T>(&self, call: cp::rpc::RpcCall, peer_id: PeerId) -> Result<T, anyhow::Error> 
        where T: DeserializeOwned
    {
        use NetworkCommand::RemoteProcedureCall;
        use cp::rpc::RpcResult::{RpcErr, RpcOk};

        let (tx, rx) = oneshot::channel();
        self.commands.send(RemoteProcedureCall{tx, call, to: Some(peer_id)});
        let ret = rx.await?;
        
        match ret {
            RpcOk(value) => Ok(serde_json::from_value(value)?),
            RpcErr(error) => bail!(error),
        }
    }

    pub async fn rpc<T>(&self, call: cp::rpc::RpcCall) -> Result<T, anyhow::Error> where T: DeserializeOwned {
        use NetworkCommand::RemoteProcedureCall;
        use cp::rpc::RpcResult::{RpcErr, RpcOk};

        let (tx, rx) = oneshot::channel();
        self.commands.send(RemoteProcedureCall{tx, call, to: None});
        let ret = rx.await?;
        
        match ret {
            RpcOk(value) => Ok(serde_json::from_value(value)?),
            RpcErr(error) => bail!(error),
        }
    }
}
pub struct NetworkActor {
    swarm: MarieSwarm,
    // Diffusion des `NetworkEvent` (voir `NetworkClient::subscribe_events`)
    events_tx: broadcast::Sender<NetworkEvent>,
    // Network command to execute
    commands_rx: mpsc::UnboundedReceiver<NetworkCommand>,
    commands_tx: mpsc::UnboundedSender<NetworkCommand>,
    // Control plane's remote procedure call -----------
    cp_peer_id: Option<PeerId>,
    /// Secret partagé par tout le cluster (voir `secret::SecretManager`). Sert
    /// à authentifier automatiquement un pair prétendant être `ControlPlane` :
    /// `agent_version` (annoncé pendant `identify`) est une simple chaîne
    /// auto-déclarée, falsifiable par n'importe qui. On exige donc en plus une
    /// preuve de possession de ce secret (défi HMAC, voir
    /// `prove_membership`/`verify_membership`) avant de faire confiance au
    /// pair — sans avoir à maintenir de liste de `PeerId` à la main.
    secret: Arc<SecretManager>,
    /// Pairs ayant déjà prouvé leur appartenance au cluster — évite de rejouer
    /// le défi à chaque événement `identify` (il peut se répéter).
    verified_control_planes: HashSet<PeerId>,
    /// Défis d'authentification envoyés, en attente de réponse : le nonce
    /// utilisé et les infos du pair à valider une fois la preuve vérifiée.
    pending_auth: HashMap<OutboundRequestId, (PeerId, [u8; 32], Option<Multiaddr>)>,
    pending_rpc: HashMap<OutboundRequestId, PendingRpc>,
    /// Handlers enregistrés dynamiquement (voir [`NetworkClient::register_rpc`]) :
    /// tout appel entrant dont le nom y figure est exécuté directement ici,
    /// sans passer par `NetworkEvent::RequestRemoteProcedureExecution`.
    handlers: HashMap<String, RpcHandler>,
}

impl NetworkActor {
    #[must_use]
    pub fn new(swarm: MarieSwarm, secret: Arc<SecretManager>) -> (Self, NetworkService) {
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let (events_tx, _) = broadcast::channel(NETWORK_EVENTS_CAPACITY);
        let local_peer_id = *swarm.local_peer_id();

        let client = NetworkService {
            commands: commands_tx.clone(),
            events: events_tx.clone(),
            local_peer_id,
            secret: secret.clone(),
        };

        let actor = NetworkActor {
            swarm,
            events_tx,
            commands_rx,
            commands_tx,
            cp_peer_id: None,
            secret,
            verified_control_planes: Default::default(),
            pending_auth: Default::default(),
            pending_rpc: Default::default(),
            handlers: Default::default()
        };

        (actor, client)
    }

    /// Fait confiance à `peer_id` en tant que control plane : c'est le seul
    /// point d'entrée vers `cp_peer_id`/`ControlPlanePeerDiscovered`, atteint
    /// soit directement (pair déjà vérifié), soit après succès du défi
    /// d'authentification (voir [`Self::challenge_control_plane`]).
    fn accept_control_plane(&mut self, peer_id: PeerId, addr: Option<Multiaddr>) {
        info!("Control plane authentifié ! PeerId: {peer_id}");
        self.verified_control_planes.insert(peer_id);
        self.cp_peer_id = Some(peer_id);

        use NetworkEvent::ControlPlanePeerDiscovered;
        let _ = self.events_tx.send(ControlPlanePeerDiscovered { peer_id, addr });
    }

    /// Envoie un défi d'authentification à `peer_id`, qui vient de se déclarer
    /// `ControlPlane` via `identify`. La vérification de la réponse se fait
    /// dans le traitement de `ReqResEvent::Message::Response` (voir `run`).
    fn challenge_control_plane(&mut self, peer_id: PeerId, addr: Option<Multiaddr>) {
        let nonce: [u8; 32] = rand::random();
        let call = RpcCall::new(RpcCall::AUTH_CHALLENGE, nonce);
        let request_id = self.swarm.behaviour_mut().rpc.send_request(&peer_id, call);
        self.pending_auth.insert(request_id, (peer_id, nonce, addr));
    }

    /// Choisit un control plane de repli parmi ceux déjà authentifiés
    /// (`verified_control_planes`) et actuellement connectés, en excluant
    /// `exclude` (le pair qui vient de se montrer injoignable).
    fn pick_fallback_control_plane(&self, exclude: PeerId) -> Option<PeerId> {
        self.verified_control_planes.iter().copied().find(|&candidate| candidate != exclude && self.swarm.is_connected(&candidate))
    }

    pub async fn run(mut self) {
        use NetworkCommand::*;
        use SwarmEvent::Behaviour;
        use request_response::Event as ReqResEvent;
        use identify::Event as IdEvent;
        use super::MarieBehaviourEvent::{Rpc, Identify, NodeGossip, Mdns, Oneway};

        loop {
            select! {
                Some(cmd) = self.commands_rx.recv() => {
                    match cmd {
                        SendFrame(mut frame) => {
                            // Le frame n'a pas de source, on va l'ajouter.
                            // Le frame peut comporter une source notamment dans un cas de forward.
                            if frame.source.is_none() {
                                frame.source = Some(serde_json::to_value(self.swarm.local_peer_id()).unwrap());
                            }
                            
                            // Si on a pas de destination mais que c'est un frame RPC, 
                            // on va le forwarder au control plane par défaut.
                            if frame.destination.is_none() 
                                && frame.channel.as_str() == "rpc" 
                                && let Some(cp_peer_id) = self.cp_peer_id 
                            {
                                warn!("rpc forwarded to the control plane");
                                frame.destination = Some(serde_json::to_value(cp_peer_id).unwrap());                                 
                            }

                            // on a pas de destinataire, c'est plus compliqué.
                            let Some(dest) = frame.destination.clone() else { 
                                warn!("cannot send frame directly because the destination is unknown, will drop it.");
                                continue;
                            };

                            let peer_id: PeerId = serde_json::from_value(dest).unwrap();
                            self.swarm.behaviour_mut().oneway.send_request(&peer_id, frame);
                        },
                        Subscribe(topic) => {
                            if let Err(error) = self.swarm.behaviour_mut().node_gossip.subscribe(&topic) {
                                warn!(%error, %topic, "abonnement gossip échoué");
                            }
                        },
                        Publish{topic, payload} => {
                            if let Err(error) = self.swarm.behaviour_mut().node_gossip.publish(topic.hash(), payload) {
                                warn!(%error, %topic, "publication gossip échouée");
                            }                            
                        },
                        RemoteProcedureCall{tx, call, to} => {

                            let retry_via_cp_failover = to.is_none();
                            let Some(peer_id) = to.or(self.cp_peer_id) else {
                                warn!("Aucun control plane n'est connu de ce noeud...");
                                continue;
                            };

                            let id = self.swarm.behaviour_mut().rpc.send_request(&peer_id, call.clone());
                            self.pending_rpc.insert(id, PendingRpc { tx, call, retry_via_cp_failover });
                        },
                        RemoteProcedureReply{channel, result} => {
                            self.swarm.behaviour_mut().rpc.send_response(channel, result);
                        },
                        IsConnected { peer_id, tx } => {
                            let _ = tx.send(self.swarm.is_connected(&peer_id));
                        },
                        RegisterHandler { name, handler } => {
                            self.handlers.insert(name, handler);
                        },
                        SubscribeGossip { topic } => {
                            if let Err(error) = self.swarm.behaviour_mut().node_gossip.subscribe(&topic) {
                                warn!(%error, %topic, "abonnement gossip échoué");
                            }
                        },
                        PublishGossip { topic, data } => {
                            if let Err(error) = self.swarm.behaviour_mut().node_gossip.publish(topic.hash(), data) {
                                warn!(%error, %topic, "publication gossip échouée");
                            }
                        },
                        Shutdown => {
                            info!("arrêt du réseau (swarm libp2p) demandé");
                            break;
                        }
                    }

                },
                event = self.swarm.select_next_some() => {
                    match event {
                        Behaviour(Oneway(ReqResEvent::Message{peer, message: request_response::Message::Request{request: mut frame, ..}, ..})) => {
                            let source = serde_json::to_value(&peer).unwrap();
                            frame.source = Some(source);
                            let _ = self.events_tx.send(NetworkEvent::ReceivedFrame(frame));
                        },
                        // Un handler a été enregistré dynamiquement pour ce nom : on
                        // l'exécute directement, sans remonter au niveau applicatif.
                        Behaviour(Rpc(ReqResEvent::Message{peer: _, message: request_response::Message::Request{request: call, channel, ..}, ..}))
                            if self.handlers.contains_key(&call.name) => {
                            let Some(handler) = self.handlers.get(&call.name).cloned() else { continue };
                            let commands_tx = self.commands_tx.clone();

                            tokio::spawn(async move {
                                use NetworkCommand::RemoteProcedureReply;

                                let result = match handler(call.args).await {
                                    Ok(value) => cp::rpc::RpcResult::RpcOk(value),
                                    Err(error) => cp::rpc::RpcResult::RpcErr(error.to_string()),
                                };
                                let _ = commands_tx.send(RemoteProcedureReply{channel, result});
                            });
                        },
                        // Sinon, remontée au niveau applicatif (voir `execute_rpc` côté
                        // control plane / worker).
                        Behaviour(Rpc(ReqResEvent::Message{peer, message: request_response::Message::Request{request: call, channel, ..}, ..}))
                         => {
                            use NetworkEvent::RequestRemoteProcedureExecution;

                            let (tx, rx) = oneshot::channel();

                            let commands_tx = self.commands_tx.clone();

                            tokio::spawn(async move {
                                use NetworkCommand::RemoteProcedureReply;
                                let Ok(result) = rx.await else {
                                    // Le `NetworkEvent` correspondant a été perdu avant d'être
                                    // traité (abonné en retard, voir `NetworkEventHandler` /
                                    // `NETWORK_EVENTS_CAPACITY`) : personne ne répondra jamais à
                                    // `tx`. L'appelant distant retentera de toute façon (voir
                                    // `FORWARD_RETRY_ATTEMPTS`).
                                    warn!("requête RPC entrante perdue avant traitement, aucune réponse envoyée");
                                    return;
                                };
                                let _ = commands_tx.send(RemoteProcedureReply{channel, result});
                            });

                            // `tx` doit être partageable (voir `RpcReplySlot`) puisque
                            // `NetworkEvent` est désormais diffusé à plusieurs abonnés
                            // potentiels : un seul doit effectivement y répondre.
                            let tx = Arc::new(Mutex::new(Some(tx)));
                            let _ = self.events_tx.send(RequestRemoteProcedureExecution{tx, call, peer});
                        },
                        // Réponse à un défi d'authentification envoyé par `challenge_control_plane`.
                        Behaviour(Rpc(ReqResEvent::Message{message: request_response::Message::Response{response: result, request_id}, ..}))
                            if self.pending_auth.contains_key(&request_id) => {
                            let Some((peer_id, nonce, addr)) = self.pending_auth.remove(&request_id) else { continue };

                            let cp::rpc::RpcResult::RpcOk(value) = result else {
                                warn!("Pair {peer_id} n'a pas su répondre au défi d'authentification, rejeté");
                                continue;
                            };
                            let Ok(proof) = serde_json::from_value::<[u8; 32]>(value) else {
                                warn!("Réponse au défi d'authentification illisible pour {peer_id}, rejeté");
                                continue;
                            };

                            match self.secret.verify_membership(&peer_id, &nonce, &proof) {
                                Ok(true) => self.accept_control_plane(peer_id, addr),
                                Ok(false) => warn!("Preuve d'appartenance invalide pour {peer_id}, rejeté en tant que control plane"),
                                Err(error) => warn!(%error, "vérification de la preuve d'appartenance impossible pour {peer_id}"),
                            }
                        },
                        // On a reçu une réponse du RPC
                        Behaviour(Rpc(ReqResEvent::Message{message: request_response::Message::Response{response: result, request_id}, ..})) => {
                            let Some(pending) = self.pending_rpc.remove(&request_id) else {continue};
                            let _ = pending.tx.send(result);
                        },
                        // La requête n'a pas pu être délivrée (pair injoignable, timeout, etc.) :
                        // il faut débloquer l'appelant — sauf s'il est possible de retenter vers
                        // un autre control plane déjà authentifié et connecté, auquel cas on ne
                        // consomme la panne que localement (un seul nouvel essai, voir `PendingRpc`).
                        Behaviour(Rpc(ReqResEvent::OutboundFailure{peer, request_id, error, ..})) => {
                            if self.pending_auth.remove(&request_id).is_some() {
                                warn!(%error, "défi d'authentification envoyé à {peer} injoignable, rejeté");
                                continue;
                            }
                            let Some(pending) = self.pending_rpc.remove(&request_id) else {continue};

                            if pending.retry_via_cp_failover {
                                if let Some(fallback) = self.pick_fallback_control_plane(peer) {
                                    warn!(%peer, %fallback, %error, "control plane injoignable, nouvel essai sur un autre control plane connu");
                                    self.cp_peer_id = Some(fallback);
                                    let id = self.swarm.behaviour_mut().rpc.send_request(&fallback, pending.call.clone());
                                    self.pending_rpc.insert(id, PendingRpc { tx: pending.tx, call: pending.call, retry_via_cp_failover: false });
                                    continue;
                                }
                            }

                            let _ = pending.tx.send(cp::rpc::RpcResult::RpcErr(format!("échec RPC vers {peer}: {error}")));
                        },
                        // mDNS ne fait que découvrir des pairs (annonce périodique sur le
                        // réseau local) — il ne les connecte pas lui-même (voir
                        // `libp2p::mdns::Behaviour`, qui ne produit jamais de
                        // `ToSwarm::Dial`). Sans ce `dial` explicite, aucune connexion
                        // libp2p ne s'établirait jamais entre deux nœuds Marie, quel que
                        // soit leur rôle : ni `identify` (qui dépend d'une connexion
                        // établie pour s'échanger, voir plus bas) ni `rpc`
                        // (`request_response`) ne pourraient jamais fonctionner.
                        Behaviour(Mdns(mdns::Event::Discovered(discovered))) => {
                            for (peer_id, addr) in discovered {
                                if self.swarm.is_connected(&peer_id) {
                                    continue;
                                }
                                if let Err(error) = self.swarm.dial(addr.clone()) {
                                    warn!(%peer_id, %addr, %error, "échec de connexion à un pair découvert par mDNS");
                                }
                            }
                        },
                        Behaviour(Identify(IdEvent::Received { peer_id, info, .. })) => {
                            let addr = info.listen_addrs.first().cloned();

                            if info.agent_version.contains("ControlPlane") {
                                // `agent_version` est une chaîne auto-déclarée par le pair, donc
                                // falsifiable : ne fait foi qu'une preuve de possession du secret
                                // partagé du cluster (voir `challenge_control_plane`).
                                if self.verified_control_planes.contains(&peer_id) {
                                    self.accept_control_plane(peer_id, addr);
                                } else {
                                    self.challenge_control_plane(peer_id, addr);
                                }
                            } else if info.agent_version.contains("Persistency") {
                                use NetworkEvent::PersistencyPeerDiscovered;
                                let _ = self.events_tx.send(PersistencyPeerDiscovered { peer_id, addr });
                            } else if info.agent_version.contains("Worker") {
                                use NetworkEvent::WorkerPeerDiscovered;
                                let _ = self.events_tx.send(WorkerPeerDiscovered { peer_id, addr });
                            }
                        },
                        Behaviour(NodeGossip(gossipsub::Event::Message { propagation_source, message, .. })) => {
                            use NetworkEvent::PubSubReceived;
                            let _ = self.events_tx.send(PubSubReceived {
                                topic: message.topic.to_string(),
                                data: message.data,
                                source: propagation_source,
                            });
                        },
                        // Plus aucune connexion établie avec ce pair (`num_established == 0` :
                        // il pouvait y en avoir plusieurs en parallèle). Signalé au niveau
                        // applicatif pour retirer ses enregistrements RPC dynamiques.
                        SwarmEvent::ConnectionClosed { peer_id, num_established: 0, .. } => {
                            // Bascule immédiatement les *futurs* appels `rpc()` vers un autre
                            // control plane connu, sans attendre qu'ils échouent une première
                            // fois (le retry sur échec, voir `OutboundFailure`, reste le filet
                            // pour les appels déjà en vol au moment de la déconnexion).
                            if self.cp_peer_id == Some(peer_id) {
                                self.cp_peer_id = self.pick_fallback_control_plane(peer_id);
                                match self.cp_peer_id {
                                    Some(fallback) => warn!(%peer_id, %fallback, "control plane déconnecté, basculement automatique"),
                                    None => warn!(%peer_id, "control plane déconnecté, aucun autre control plane connu et joignable"),
                                }
                            }

                            use NetworkEvent::PeerDisconnected;
                            let _ = self.events_tx.send(PeerDisconnected { peer_id });
                        },
                        _ => {}
                    }
                }

            }
        }
    
    }
}