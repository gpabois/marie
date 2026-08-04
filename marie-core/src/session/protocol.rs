use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    events::Event, expert::RequestAskExpert, graph::{GraphId, NodeId}, hitl::{Hitl, protocol::{HitlRequest, HitlResponse}}, job::JobState, session::{SessionId, SessionStatus, channel::{ChannelName, ChannelUpdate}, frames::FrameId, logs::SessionLog, run_log::RunLog}, shell::ShellMode, tools::RequestToolCall
};

#[derive(Clone, Deserialize, Serialize)]
pub enum NewSessionArgs {
    Shell(ShellMode),
    Graph {
        graph_id: GraphId,
        initial: HashMap<ChannelName, Value>
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FrameResponse {
    pub frame_id: FrameId,
    pub updates: Vec<ChannelUpdate>,
    pub new_logs: Vec<RunLog>,
    pub result: FrameResult
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Branch {
    pub start: NodeId,
    pub overrides: HashMap<ChannelName, Value>
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FrameResult {
    // --- Commandes HITL ------ //
    RequestHitl(Hitl),
    /// Demande l'avis d'experts
    ConsultExperts(Vec<RequestAskExpert>),
    /// Demande l'exécution d'outils
    CallTools(Vec<RequestToolCall>),
    // --- Commandes de graphe ------ //
    ExecuteGraph(GraphId),
    /// Va au prochain noeud
    /// Cela ajoute un frame 
    Continue,
    /// Va à un noeud nommément désigné
    GoTo(NodeId),
    /// Fourche sur plusieurs noeuds en simultané
    Fork {
        /// les branches à crééer
        branches: Vec<Branch>,
        /// le noeud rendez-vous des branches
        join: NodeId
    },
    // --- Commandes générales ------ //
    /// Le noeud a terminé, rien besoin de faire de plus
    Completed,
    /// Le noeud a échoué
    Failed(String)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionEvent {
    HitlAnswered(HitlResponse),
    HitlRequested(HitlRequest),
    LogCreated(SessionLog),
    LogUpdated(SessionLog),
    /// Émis à chaque écriture de [`SessionStatus`] (voir
    /// [`crate::session::checkpointer::SessionCheckpointer::fail`], seul
    /// site d'écriture existant à ce jour) — permet à un consommateur
    /// attaché à une session (voir [`crate::session::client::SessionClient`])
    /// de savoir qu'elle est terminée sans avoir à re-sonder
    /// [`crate::session::dto::SessionView::status`].
    SessionStatusUpdated(SessionId, SessionStatus)
}

impl SessionEvent {
    pub fn session_id(&self) -> SessionId {
        match self {
            SessionEvent::HitlRequested(hitl_request) => hitl_request.session_id,
            SessionEvent::LogCreated(session_log) => session_log.session_id,
            SessionEvent::LogUpdated(session_lod) => session_lod.session_id,
            SessionEvent::HitlAnswered(hitl_response) => hitl_response.session_id,
            SessionEvent::SessionStatusUpdated(session_id, _) => *session_id,
        }
    }
}

impl SessionEvent {
    pub fn session_scope_topic(session_id: SessionId) -> String {
        format!("/marie/events/sessions/{}", session_id)
    }
}

impl Event for SessionEvent {
    const TOPIC: &str = "/marie/events/sessions";

    fn id(&self) -> String {
        match self {
            SessionEvent::HitlRequested(hitl_request) => hitl_request.id.to_string(),
            SessionEvent::LogCreated(session_log) => session_log.id.to_string(),
            SessionEvent::LogUpdated(session_log) => session_log.id.to_string(),
            SessionEvent::HitlAnswered(hitl_response) => hitl_response.session_id.to_string(),
            SessionEvent::SessionStatusUpdated(session_id, _) => session_id.to_string(),
        }
    }

    fn topics(&self) -> Vec<String> {
        vec![
            Self::TOPIC.to_string(),
            Self::session_scope_topic(self.session_id())
        ]
    }
}

pub(crate) enum SessionCheckpointEvent {
    SessionCreated {
        session_id: SessionId
    },
    HitlAnswered {
        session_id: SessionId,
        response: HitlResponse
    },
    /// Un frame vient d'être créé, voir [`SessionHandler::on_frame_created`].
    FrameCreated {
        session_id: SessionId,
        frame_id: FrameId
    },
    /// Mise à jour intermédiaire d'un run en cours, voir
    /// [`SessionHandler::on_frame_run_update`].
    FrameRunJobStateUpdate {
        session_id: SessionId,
        frame_id: FrameId,
        job_state: JobState<FrameResponse>,
    },
    /// The frame is ready to run
    FrameReady {
        session_id: SessionId,
        frame_id: FrameId,
    },
    /// Un run vient de se terminer (complété ou échoué), voir
    /// [`SessionHandler::on_frame_run_terminated`].
    FrameRunTerminated {
        session_id: SessionId,
        frame_id: FrameId,
    },
    /// Un enfant de `parent_id` vient de terminer, voir
    /// [`SessionHandler::on_child_frame_terminated`].
    ChildFrameTerminated {
        session_id: SessionId,
        parent_id: FrameId,
        child_id: FrameId
    },
    /// A frame has terminated
    FrameTerminated {
        session_id: SessionId,
        frame_id: FrameId
    }
}

impl SessionCheckpointEvent {
    pub fn session_id(&self) -> SessionId {
        match self {
            SessionCheckpointEvent::SessionCreated { session_id } => *session_id,
            SessionCheckpointEvent::HitlAnswered { session_id, response } => *session_id,
            SessionCheckpointEvent::FrameCreated { session_id, frame_id } => *session_id,
            SessionCheckpointEvent::FrameRunJobStateUpdate { session_id, frame_id, job_state } => *session_id,
            SessionCheckpointEvent::FrameReady { session_id, frame_id } => *session_id,
            SessionCheckpointEvent::FrameRunTerminated { session_id, frame_id } => *session_id,
            SessionCheckpointEvent::ChildFrameTerminated { session_id, parent_id, child_id } => *session_id,
            SessionCheckpointEvent::FrameTerminated { session_id, frame_id } => *session_id,
        }
    }
}