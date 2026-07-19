//! Vocabulaire minimal échangé sur le websocket de la passerelle (voir
//! [`crate::ws`]). Volontairement réduit à ce qu'un client web a besoin de
//! déclencher/observer sur une session ou un formulaire HITL — un
//! appelant qui veut faire transiter d'autres messages sur le même socket
//! (états applicatifs propres à son produit, notifications, ...) définit
//! son propre type et les multiplexe lui-même (voir la doc du crate) : nous
//! ne cherchons pas à anticiper ce vocabulaire ici.
//!
//! `#[serde(tag = "type")]` sur les deux enums : un client JS/TS peut
//! discriminer un message reçu sans bibliothèque de désérialisation dédiée.

use std::collections::HashMap;

use marie_core::{
    agent::{context::ContextEntry, frame::AgentFrame, status::AgentStatus},
    hitl::{Answer, HumanInputRequest},
    id::ID,
    job::Job,
    mode::SessionMode,
    session::{SessionId, client::SessionEvent},
};
use serde::{Deserialize, Serialize};

/// Message envoyé par le client WebSocket vers la passerelle.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// S'abonner aux événements de cycle de vie de `session_id` (voir
    /// [`SessionEvent`]) — jusqu'à la déconnexion ou un
    /// [`Self::UnsubscribeSession`] explicite. Sans effet si déjà abonné
    /// (voir `SessionClient::acquire`).
    SubscribeSession { session_id: SessionId },
    /// Retire la session détenue localement par cette passerelle (voir
    /// `SessionClient::remove`) — purement local, sans effet sur les autres
    /// détenteurs du cluster.
    UnsubscribeSession { session_id: SessionId },
    /// Récupère un instantané du frame `local_id` de `session_id` — répond
    /// par [`ServerMessage::Frame`].
    GetFrame { session_id: SessionId, local_id: ID },
    /// Répond à un formulaire HITL préalablement reçu via
    /// [`ServerMessage::HitlRequest`] (voir `HitlClient::answer`).
    HitlAnswer { request_id: ID, answers: HashMap<String, Answer> },
    /// Soumet un job au control plane (voir `NetworkClient::spawn_job`).
    SubmitJob { job: Job },
    /// Injecte `text` comme nouveau message utilisateur : crée un nouveau
    /// frame dans `session_id` (voir `SessionClient::put_frame`) avec `text`
    /// comme unique message initial, puis soumet un job
    /// [`marie_core::job::JobKind::RunAgent`] pour ce frame — répond par
    /// [`ServerMessage::MessageSent`] avec l'identifiant du frame créé.
    /// Fonctionne aussi bien pour un mode [`SessionMode::Simple`] (l'agent
    /// démarré répond directement) que pour [`SessionMode::Orchestration`]
    /// (voir `network::worker::mod::run_orchestration` côté `marie-core` :
    /// ce nouveau frame en devient l'orchestrateur, qui délègue `text` à un
    /// enfant) ; n'a pas de sens pour [`SessionMode::StateGraph`], dont
    /// l'exécution suit le graphe plutôt qu'un message libre.
    SendMessage { session_id: SessionId, model_id: String, allowed_tools: Vec<String>, text: String },
    /// Récupère le mode actuellement au sommet de la pile de `session_id`
    /// (voir [`SessionMode`]) — répond par [`ServerMessage::Mode`].
    GetMode { session_id: SessionId },
}

/// Message envoyé par la passerelle vers le client WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    /// Un événement de cycle de vie de session gossipé sur le cluster — reçu
    /// pour toute session abonnée via [`ClientMessage::SubscribeSession`],
    /// pas seulement celles détenues localement par cette passerelle (voir
    /// `session::client::SessionClient::subscribe`).
    SessionEvent(SessionEvent),
    /// Un formulaire HITL soumis par un agent du cluster, à présenter à
    /// l'opérateur humain — voir `HitlClient::subscribe_requests`.
    HitlRequest(HumanInputRequest),
    /// Réponse à [`ClientMessage::GetFrame`] — `frame` est `None` si la
    /// session ou le frame demandé sont inconnus de cette passerelle (voir
    /// `SessionClient::frame`).
    Frame { session_id: SessionId, local_id: ID, frame: Option<FrameSnapshot> },
    /// Confirme l'exécution réussie d'un [`ClientMessage`] qui n'a pas de
    /// réponse dédiée (ex: [`ClientMessage::SubscribeSession`]) —
    /// `in_reply_to` reprend le nom de variante concerné, pour permettre au
    /// client de corréler sans identifiant de requête dédié. `String` plutôt
    /// que `&'static str` : un champ emprunté empêcherait `ServerMessage`
    /// de dériver `Deserialize` (utile côté tests), pour un coût mémoire
    /// négligeable au vu de la fréquence de ces messages.
    Ack { in_reply_to: String },
    /// Échec d'exécution d'un [`ClientMessage`] — même principe que
    /// [`Self::Ack`] pour `in_reply_to`.
    Error { in_reply_to: String, message: String },
    /// Réponse à [`ClientMessage::SendMessage`] — `local_id` est celui du
    /// frame nouvellement créé, pour que le client puisse s'y abonner/
    /// l'afficher sans avoir à le deviner.
    MessageSent { session_id: SessionId, local_id: ID },
    /// Réponse à [`ClientMessage::GetMode`].
    Mode { session_id: SessionId, mode: SessionMode },
}

/// Copie sérialisable d'un [`AgentFrame`] — `AgentFrame` ne dérive pas
/// `Serialize`/`Deserialize` (`marie-core` ne l'expose qu'en mémoire, voir
/// `session::client::SessionClient::frame`), cette copie est donc
/// reconstruite champ à champ pour transiter sur le websocket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrameSnapshot {
    pub session_id: SessionId,
    pub id: ID,
    pub model_id: String,
    pub status: AgentStatus,
    pub allowed_tools: Vec<String>,
    pub context: Vec<ContextEntry>,
    pub stdio: String,
    pub stderr: String,
}

impl From<&AgentFrame> for FrameSnapshot {
    fn from(frame: &AgentFrame) -> Self {
        Self {
            session_id: frame.session_id,
            id: frame.id,
            model_id: frame.model.clone(),
            status: frame.status.clone(),
            allowed_tools: frame.allowed_tools.clone(),
            context: frame.context.to_vec(),
            stdio: frame.stdio.clone(),
            stderr: frame.stderr.clone(),
        }
    }
}
