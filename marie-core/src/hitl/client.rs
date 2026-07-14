use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt as _};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, broadcast, oneshot};
use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};
use tracing::debug;

use crate::{
    agent::GlobalAgentId,
    hitl::{ASK_HUMAN_TOOL, Answer, HitlError, HumanInputAnswer, HumanInputRequest, Question, tool_declaration},
    id::ID,
    network::actor::{NetworkService, NetworkEvent, NetworkEventHandler},
    tools::{client::ToolClient, declaration::ToolId},
};

/// Capacité du canal de diffusion locale des [`HumanInputRequest`] (voir
/// [`HitlClient::subscribe_requests`]) — sur le même modèle que
/// `session::client::SESSION_EVENTS_CAPACITY` : peu
/// fréquent, une capacité modeste suffit à absorber un abonné
/// temporairement en retard.
const HITL_REQUESTS_CAPACITY: usize = 64;

/// Topic gossipsub (`node_gossip`) sur lequel formulaires ([`HumanInputRequest`])
/// et réponses ([`HumanInputAnswer`]) transitent — voir le module
/// [`crate::hitl`] pour la justification de ce découplage par rapport au
/// relais RPC point-à-point utilisé par les autres tools.
pub const HITL_TOPIC: &str = "marie/hitl/1.0.0";

/// `pub(crate)` plutôt que privé : `network::cp::mod` a besoin de
/// reconnaître une [`HumanInputAnswer`] sur [`HITL_TOPIC`] pour reprendre un
/// agent qui l'attendait (voir `network::cp::mod::resume_after_hitl_answer`)
/// — le format du wire ne doit exister qu'à un seul endroit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum HitlGossipMessage {
    Request(HumanInputRequest),
    Answer(HumanInputAnswer),
}

/// Flux de [`HumanInputRequest`] retourné par
/// [`HitlClient::subscribe_requests`] — même motif que
/// `session::client::SessionEventHandler` : un abonné en
/// retard perd les formulaires les plus anciens (`Lagged`), absorbé
/// silencieusement plutôt que remonté comme une erreur.
pub struct HitlRequestHandler(BroadcastStream<HumanInputRequest>);

impl Stream for HitlRequestHandler {
    type Item = HumanInputRequest;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            return match Pin::new(&mut self.0).poll_next(cx) {
                Poll::Ready(Some(Ok(request))) => Poll::Ready(Some(request)),
                Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(skipped)))) => {
                    debug!(skipped, "abonné HumanInputRequest en retard, formulaires perdus");
                    continue;
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            };
        }
    }
}

/// Point d'entrée du tool [`crate::hitl::ASK_HUMAN_TOOL`], côté agent (voir
/// [`Self::ask`]) comme côté passerelle humaine (voir
/// [`Self::subscribe_requests`]/[`Self::answer`]) — voir le module
/// [`crate::hitl`] pour la justification du transport gossip plutôt que RPC.
///
/// Bon marché à cloner (comme `NetworkClient`/`SessionClient`) : pensé pour
/// être threadé dans les tâches de fond au même titre qu'eux.
#[derive(Clone)]
pub struct HitlClient {
    network: NetworkService,
    /// Formulaires en vol émis par ce nœud, en attente de leur
    /// [`HumanInputAnswer`] — retirée dès la première réponse reçue (voir
    /// [`ingest_network_events`]), les suivantes pour le même `id` sont
    /// alors ignorées ("le premier qui répond l'emporte", comme
    /// `network::cp::forward_race`).
    pending: Arc<Mutex<HashMap<ID, oneshot::Sender<HashMap<String, Answer>>>>>,
    requests: broadcast::Sender<HumanInputRequest>,
}

impl HitlClient {
    /// S'abonne au flux d'événements réseau de `network` (voir
    /// `NetworkClient::subscribe_events`) et démarre sa propre tâche de fond
    /// pour dispatcher les messages gossipés sur [`HITL_TOPIC`] — l'appelant
    /// n'a donc pas besoin de savoir filtrer quoi que ce soit lui-même.
    #[must_use]
    pub fn new(network: NetworkService) -> Self {
        network.subscribe(HITL_TOPIC);

        let pending = Arc::new(Mutex::new(HashMap::new()));
        let (requests, _) = broadcast::channel(HITL_REQUESTS_CAPACITY);

        tokio::spawn(ingest_network_events(network.subscribe_events(), pending.clone(), requests.clone()));

        Self { network, pending, requests }
    }

    /// Enregistre (ou remplace) la déclaration de [`crate::hitl::ASK_HUMAN_TOOL`]
    /// dans le catalogue de tools (voir `crate::hitl::tool_declaration`) —
    /// idempotent, à appeler une fois lors de la configuration du cluster
    /// (comme on déclarerait un modèle). Sans cela, le tool reste utilisable
    /// via [`Self::ask`] mais invisible du modèle (voir
    /// [`crate::model::execute`], qui ne fournit au modèle que les
    /// signatures listées dans `crate::tools::catalog::ToolCatalog`).
    pub async fn ensure_declared(&self, tools: &ToolClient) -> Result<(), HitlError> {
        tools.set(ToolId::from(ASK_HUMAN_TOOL), tool_declaration()).await.map_err(|error| HitlError::Network(error.to_string()))
    }

    /// Soumet `questions` au premier humain disponible et attend l'ensemble
    /// des réponses (voir [`Question`]/[`Answer`]), sans limite de temps
    /// imposée par le transport (voir le module [`crate::hitl`]) — seul
    /// l'abandon de ce `Future` par l'appelant (ex. timeout applicatif,
    /// annulation du job) interrompt l'attente.
    ///
    /// Si ce nœud est détruit (donc la tâche de fond démarrée par
    /// [`Self::new`] aussi) avant qu'une réponse n'arrive, retourne
    /// [`HitlError::Cancelled`] plutôt que d'attendre indéfiniment.
    pub async fn ask(&self, agent_id: GlobalAgentId, questions: Vec<Question>) -> Result<HashMap<String, Answer>, HitlError> {
        let id = crate::id::generate_id();
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);

        let request = HumanInputRequest { id, agent_id, questions };
        if let Err(error) = self.network.publish(HITL_TOPIC, HitlGossipMessage::Request(request)) {
            self.pending.lock().await.remove(&id);
            return Err(HitlError::Network(error.to_string()));
        }

        rx.await.map_err(|_| HitlError::Cancelled)
    }

    /// Publie `questions` sous l'identifiant `id` sans attendre de réponse —
    /// pour un appelant qui va terminer son run et se laisser reprendre plus
    /// tard (voir `agent::run`, qui yielde sur
    /// [`crate::agent::status::YieldStatus::WaitingToolReply`] plutôt que
    /// d'attendre en place comme [`Self::ask`], pour la même raison que
    /// [`crate::mode::executable::NodeOutcome::Yield`] côté `StateGraph`).
    /// `id` doit être le `tool_call_id` de l'appel de tool
    /// [`crate::hitl::ASK_HUMAN_TOOL`] d'origine : c'est ce qui permet à
    /// [`network::cp::mod::resume_after_hitl_answer`](crate::network::cp)
    /// de corréler la [`HumanInputAnswer`] reçue à l'agent qui attendait,
    /// sans registre séparé.
    pub fn ask_and_forget(&self, id: ID, agent_id: GlobalAgentId, questions: Vec<Question>) -> Result<(), HitlError> {
        let request = HumanInputRequest { id, agent_id, questions };
        self.network.publish(HITL_TOPIC, HitlGossipMessage::Request(request)).map_err(|error| HitlError::Network(error.to_string()))
    }

    /// S'abonne aux formulaires soumis par les agents du cluster (voir
    /// [`HumanInputRequest`]) — à consommer par une passerelle humaine (voir
    /// `node::Marie::join`, ex. une passerelle HTTP/WebSocket) pour les
    /// présenter à un opérateur, puis y répondre via [`Self::answer`].
    /// Chaque abonné reçoit sa propre copie ; les formulaires émis avant
    /// l'abonnement ne sont pas rejoués.
    #[must_use]
    pub fn subscribe_requests(&self) -> HitlRequestHandler {
        HitlRequestHandler(BroadcastStream::new(self.requests.subscribe()))
    }

    /// Répond au formulaire `request` (voir [`HumanInputRequest::id`]) —
    /// `answers` est d'abord validée contre les questions d'origine (voir
    /// [`HumanInputRequest::validate`]) : une réponse mal formée n'est
    /// jamais publiée. Diffusion best-effort ensuite, comme le reste de ce
    /// module : l'absence de pair intéressé (agent déjà reparti, réponse
    /// concurrente déjà acceptée) ne fait pas échouer l'appel.
    pub async fn answer(&self, request: &HumanInputRequest, answers: HashMap<String, Answer>) -> Result<(), HitlError> {
        request.validate(&answers)?;

        let message = HitlGossipMessage::Answer(HumanInputAnswer { request_id: request.id, answers });
        self.network.publish(HITL_TOPIC, message).map_err(|error| HitlError::Network(error.to_string()))
    }
}

/// Tâche de fond démarrée par [`HitlClient::new`] : consomme
/// `network_events` et, pour tout message gossipé sur [`HITL_TOPIC`],
/// réémet les formulaires vers les abonnés locaux (voir
/// [`HitlClient::subscribe_requests`]) et résout le formulaire en attente
/// correspondant à toute réponse reçue (voir [`HitlClient::ask`]). Tout
/// événement qui n'est pas un `GossipMessageReceived` sur ce topic est
/// ignoré silencieusement.
async fn ingest_network_events(
    mut network_events: NetworkEventHandler,
    pending: Arc<Mutex<HashMap<ID, oneshot::Sender<HashMap<String, Answer>>>>>,
    requests: broadcast::Sender<HumanInputRequest>,
) {
    while let Some(event) = network_events.next().await {
        let NetworkEvent::GossipMessageReceived { topic, data, .. } = event else {
            continue;
        };

        if topic != HITL_TOPIC {
            continue;
        }

        let Ok(message) = serde_json::from_slice::<HitlGossipMessage>(&data) else {
            continue;
        };

        match message {
            HitlGossipMessage::Request(request) => {
                let _ = requests.send(request);
            }
            HitlGossipMessage::Answer(answer) => {
                if let Some(tx) = pending.lock().await.remove(&answer.request_id) {
                    let _ = tx.send(answer.answers);
                }
            }
        }
    }
}
