//! Websocket de la passerelle — voir la doc du crate pour la philosophie
//! (pas d'authentification imposée, briques composables plutôt qu'une
//! boucle figée).
//!
//! [`serve`] est la boucle "batteries incluses" : elle pousse [`events`]
//! vers le client et lui répond via [`dispatch`] pour chaque
//! [`ClientMessage`] reçu. Un appelant qui a besoin de mélanger d'autres
//! sources de messages sur le même socket (voir la doc du crate) n'utilise
//! pas [`serve`] — il écrit sa propre boucle `tokio::select!` autour
//! d'[`events`]/[`dispatch`], typiquement démarrée depuis son propre
//! handler `axum` après sa propre étape d'authentification :
//!
//! ```ignore
//! async fn ws_handler(
//!     ws: WebSocketUpgrade,
//!     AuthUser(user): AuthUser, // extracteur d'authentification de l'appelant
//!     State(state): State<GatewayState>,
//! ) -> impl IntoResponse {
//!     ws.on_upgrade(move |socket| async move {
//!         // boucle personnalisée, mêlant `events(&state, ...)` à d'autres flux
//!     })
//! }
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::IntoResponse,
    routing::get,
};
use futures::{Sink, SinkExt as _, Stream, StreamExt as _};
use marie_core::{
    agent::{GlobalAgentId, context::ContextEntry, frame::AgentFrame, role::Role, status::AgentStatus},
    hitl::HumanInputRequest,
    id::{ID, generate_id},
    job::{Job, JobKind},
    session::client::SessionClient,
};
use tokio::sync::RwLock;
use tracing::debug;

use crate::{
    gateway::MarieGateway,
    protocol::{ClientMessage, FrameSnapshot, ServerMessage},
};

/// État partagé d'une route websocket "batteries incluses" (voir [`router`]) —
/// à composer dans son propre état `axum` (via un champ, ou
/// `axum::extract::FromRef`) plutôt qu'à réutiliser tel quel si l'appelant a
/// besoin d'autre chose dans son état (base de données applicative,
/// session d'authentification, ...).
#[derive(Clone)]
pub struct GatewayState {
    pub gateway: MarieGateway,
    pub sessions: SessionClient,
}

/// Formulaires HITL transmis à une connexion, en attente de réponse —
/// nécessaire parce que `HitlClient::answer` exige le [`HumanInputRequest`]
/// d'origine (pour revalider la réponse contre son formulaire, voir sa
/// doc), pas seulement son identifiant. Une instance par connexion : un
/// formulaire que ce client n'a jamais vu (jamais poussé par [`events`] sur
/// *son* socket) ne peut pas être répondu depuis celui-ci.
#[derive(Clone, Default)]
pub struct HitlRegistry(Arc<RwLock<HashMap<ID, HumanInputRequest>>>);

impl HitlRegistry {
    async fn remember(&self, request: &HumanInputRequest) {
        self.0.write().await.insert(request.id, request.clone());
    }

    async fn take(&self, request_id: ID) -> Option<HumanInputRequest> {
        self.0.write().await.remove(&request_id)
    }
}

/// Fusionne le flux HITL global (voir `HitlClient::subscribe_requests`) et
/// le flux d'événements de session (voir `SessionClient::subscribe`) en un
/// seul flux de [`ServerMessage`] — fourni comme brique séparée (voir la
/// doc du module) plutôt qu'enfouie dans [`serve`]. Alimente `registry` au
/// passage : un formulaire HITL doit y être enregistré avant que le client
/// ne puisse y répondre (voir [`dispatch`]).
pub fn events(state: &GatewayState, registry: HitlRegistry) -> impl Stream<Item = ServerMessage> + Send + 'static {
    let hitl_requests = state.gateway.hitl_client().subscribe_requests();
    let hitl = async_stream_hitl(hitl_requests, registry).map(ServerMessage::HitlRequest);
    let session = state.sessions.subscribe().map(ServerMessage::SessionEvent);
    futures::stream::select(hitl, session)
}

/// Enregistre chaque [`HumanInputRequest`] dans `registry` avant de le
/// laisser passer — un simple `.map()` ne suffirait pas, l'enregistrement
/// est un effet de bord qui doit se produire avant que [`ServerMessage::HitlRequest`]
/// n'atteigne le client (sans quoi une réponse immédiate côté client
/// pourrait arriver avant l'enregistrement).
fn async_stream_hitl(
    mut requests: impl Stream<Item = HumanInputRequest> + Unpin + Send + 'static,
    registry: HitlRegistry,
) -> impl Stream<Item = HumanInputRequest> + Send + 'static {
    async_stream::stream! {
        while let Some(request) = requests.next().await {
            registry.remember(&request).await;
            yield request;
        }
    }
}

/// Exécute une [`ClientMessage`] reçue sur le websocket et produit la
/// réponse à renvoyer — chaque branche délègue à un client `marie-core` qui
/// porte déjà ses propres délais/retries, jamais bloquant indéfiniment.
pub async fn dispatch(state: &GatewayState, registry: &HitlRegistry, msg: ClientMessage) -> ServerMessage {
    match msg {
        ClientMessage::SubscribeSession { session_id } => match state.sessions.acquire(session_id).await {
            Ok(()) => ServerMessage::Ack { in_reply_to: "subscribe_session".to_string() },
            Err(error) => ServerMessage::Error { in_reply_to: "subscribe_session".to_string(), message: error.to_string() },
        },
        ClientMessage::UnsubscribeSession { session_id } => {
            state.sessions.remove(session_id).await;
            ServerMessage::Ack { in_reply_to: "unsubscribe_session".to_string() }
        }
        ClientMessage::GetFrame { session_id, local_id } => {
            let frame = state.sessions.frame(session_id, local_id).await;
            ServerMessage::Frame { session_id, local_id, frame: frame.as_ref().map(FrameSnapshot::from) }
        }
        ClientMessage::HitlAnswer { request_id, answers } => {
            let Some(request) = registry.take(request_id).await else {
                return ServerMessage::Error {
                    in_reply_to: "hitl_answer".to_string(),
                    message: "formulaire inconnu de cette connexion (jamais reçu, ou déjà répondu)".to_string(),
                };
            };
            match state.gateway.hitl_client().answer(&request, answers).await {
                Ok(()) => ServerMessage::Ack { in_reply_to: "hitl_answer".to_string() },
                Err(error) => ServerMessage::Error { in_reply_to: "hitl_answer".to_string(), message: error.to_string() },
            }
        }
        ClientMessage::SubmitJob { job } => match state.gateway.network().spawn_job(job).await {
            Ok(()) => ServerMessage::Ack { in_reply_to: "submit_job".to_string() },
            Err(error) => ServerMessage::Error { in_reply_to: "submit_job".to_string(), message: error.to_string() },
        },
        ClientMessage::SendMessage { session_id, model_id, allowed_tools, text } => {
            let local_id = generate_id();
            let frame = AgentFrame {
                session_id,
                id: local_id,
                model_id,
                status: AgentStatus::Initial,
                allowed_tools,
                context: vec![ContextEntry { role: Role::User, content: text }].into(),
                stdio: String::new(),
                stderr: String::new(),
            };

            if let Err(error) = state.sessions.put_frame(session_id, local_id, &frame).await {
                return ServerMessage::Error { in_reply_to: "send_message".to_string(), message: error.to_string() };
            }

            let job = Job { id: generate_id(), kind: JobKind::RunAgent(GlobalAgentId::new(session_id, local_id)) };
            match state.gateway.network().spawn_job(job).await {
                Ok(()) => ServerMessage::MessageSent { session_id, local_id },
                Err(error) => ServerMessage::Error { in_reply_to: "send_message".to_string(), message: error.to_string() },
            }
        }
        ClientMessage::GetMode { session_id } => {
            let mode = state.sessions.current_mode(session_id).await;
            ServerMessage::Mode { session_id, mode }
        }
    }
}

/// Route `/ws` prête à l'emploi, **sans authentification** : point de départ
/// pour prototyper vite, ou modèle à copier pour brancher sa propre
/// sécurité (voir la doc du crate) — à ne monter que derrière sa propre
/// couche d'authentification (middleware, garde de réseau, ...), jamais
/// exposée telle quelle sur un réseau non fiable.
pub fn router(state: GatewayState) -> Router {
    Router::new().route("/ws", get(ws_handler)).with_state(state)
}

async fn ws_handler(State(state): State<GatewayState>, ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(move |socket| serve(socket, state))
}

/// Boucle websocket "batteries incluses" (voir la doc du module) — un
/// message texte qui ne désérialise pas en [`ClientMessage`] est ignoré
/// silencieusement : un appelant qui veut le traiter (vocabulaire propre à
/// son produit, voir la doc du crate) écrit sa propre boucle plutôt que
/// d'utiliser celle-ci.
pub async fn serve(socket: WebSocket, state: GatewayState) {
    let (mut sink, mut stream) = socket.split();
    let registry = HitlRegistry::default();
    let mut server_events = Box::pin(events(&state, registry.clone()));

    loop {
        tokio::select! {
            event = server_events.next() => {
                let Some(event) = event else { break };
                if send(&mut sink, &event).await.is_err() { break; }
            }
            frame = stream.next() => {
                let Some(Ok(frame)) = frame else { break };
                let Message::Text(text) = frame else { continue };
                let msg = match serde_json::from_str::<ClientMessage>(&text) {
                    Ok(msg) => msg,
                    Err(error) => {
                        debug!(%error, "message websocket illisible, ignoré (voir GatewayState/serve)");
                        continue;
                    }
                };
                let response = dispatch(&state, &registry, msg).await;
                if send(&mut sink, &response).await.is_err() { break; }
            }
        }
    }
}

async fn send(sink: &mut (impl Sink<Message> + Unpin), msg: &ServerMessage) -> Result<(), ()> {
    let Ok(text) = serde_json::to_string(msg) else { return Ok(()) };
    sink.send(Message::Text(text.into())).await.map_err(|_| ())
}
