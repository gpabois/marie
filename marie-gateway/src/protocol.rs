use std::collections::HashMap;

use marie_core::{
    hitl::Answer,
    session::{Session, SessionEvent, SessionId, state::hitl::HitlFrameId},
    workspace::{Workspace, WorkspaceEvent, WorkspaceId},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Commandes qu'un utilisateur externe (au travers de `foreign`, voir
/// `MarieGatewayActor::create`) peut envoyer à la passerelle. Toute variante
/// autre que `AccessSession`/`AccessWorkspace`/`CreateWorkspace`/`CreateSession`
/// n'est traitée que si l'id qu'elle porte a préalablement été autorisé
/// (voir [`Self::required_session`]/[`Self::required_workspace`]) — sinon
/// `MarieGatewayActor` la laisse tomber (voir
/// [`MarieGatewayEvent::CommandRejected`]), sans jamais l'exécuter contre
/// `SessionClient`/`WorkspaceClient`.
///
/// Volontairement ABSENTES (usage interne au runtime d'agent, jamais à un
/// utilisateur externe) :
/// - `SessionClient::update` (remplacement complet, réservé au serveur de
///   sessions lui-même).
/// - `report_agent_run`/`report_tool_dispatch`/`report_tool_execution`
///   (comptes-rendus de fin de `Job`, jamais déclenchés par un utilisateur).
/// - `insert_in_log` (accumulation de streaming interne — `AppendSessionLog`
///   couvre le seul besoin utilisateur, ajouter une ligne de journal).
/// - `push_graph`/`update_graph_step`/`report_graph_dispatch`/
///   `report_graph_run`/`push_orchestration`/`push_hitl` (mutations internes
///   du runtime d'agent/graphe/orchestration/HITL — observées en LECTURE
///   SEULE via les `SessionEvent` correspondants, jamais déclenchées depuis
///   la passerelle).
/// - `SessionClient::list`/`WorkspaceClient::list` (énumération de TOUT le
///   catalogue cluster — incompatible avec un modèle d'autorisation par id).
#[derive(Debug, Serialize, Deserialize)]
pub enum MarieGatewayCommand {
    // --- Autorisation ---
    AccessSession(SessionId),
    AccessWorkspace(WorkspaceId),

    // --- Session (nécessite AccessSession(session_id) au préalable) ---
    GetSession(SessionId),
    RemoveSession(SessionId),
    AppendSessionLog { session_id: SessionId, line: String },
    QuerySessionVars { session_id: SessionId, path: String },
    PatchSessionVars { session_id: SessionId, path: String, value: Value },
    ReportUserInput { session_id: SessionId, hitl_id: Option<HitlFrameId>, answers: HashMap<String, Answer> },

    // --- Workspace ---
    /// Ne nécessite aucune autorisation préalable : un workspace qui n'existe
    /// pas encore ne peut pas avoir été autorisé — son créateur est autorisé
    /// d'office dessus une fois créé (voir `MarieGatewayActor::dispatch`).
    CreateWorkspace(WorkspaceId),
    /// Nécessite AccessWorkspace(workspace_id) au préalable.
    GetWorkspace(WorkspaceId),
    RemoveWorkspace(WorkspaceId),
    ListWorkspaceSessions(WorkspaceId),
    QueryWorkspaceVars { workspace_id: WorkspaceId, path: String },
    PatchWorkspaceVars { workspace_id: WorkspaceId, path: String, value: Value },
    AddSessionToWorkspace { workspace_id: WorkspaceId, session_id: SessionId },
    RemoveSessionFromWorkspace { workspace_id: WorkspaceId, session_id: SessionId },
    /// Crée une session fraîche ET la rattache à `workspace_id` — enchaîne
    /// `WorkspaceClient::create_session` puis `SessionClient::insert` d'une
    /// `Session` vide (il n'existe pas de `Session::new()`). Nécessite
    /// AccessWorkspace(workspace_id) au préalable ; la session nouvellement
    /// créée est elle-même autorisée d'office pour cette connexion (voir
    /// `MarieGatewayActor::handle_create_session`).
    CreateSession(WorkspaceId),
}

impl MarieGatewayCommand {
    /// Session devant déjà être autorisée pour que cette commande soit
    /// traitée — `None` pour les commandes d'accès/création et les
    /// commandes purement workspace.
    pub(crate) fn required_session(&self) -> Option<SessionId> {
        use MarieGatewayCommand::*;
        match self {
            GetSession(id) | RemoveSession(id) => Some(*id),
            AppendSessionLog { session_id, .. }
            | QuerySessionVars { session_id, .. }
            | PatchSessionVars { session_id, .. }
            | ReportUserInput { session_id, .. } => Some(*session_id),
            _ => None,
        }
    }

    /// Workspace devant déjà être autorisé pour que cette commande soit
    /// traitée — `None` pour les commandes d'accès/création et les
    /// commandes purement session.
    pub(crate) fn required_workspace(&self) -> Option<WorkspaceId> {
        use MarieGatewayCommand::*;
        match self {
            GetWorkspace(id) | RemoveWorkspace(id) | ListWorkspaceSessions(id) | CreateSession(id) => Some(*id),
            QueryWorkspaceVars { workspace_id, .. }
            | PatchWorkspaceVars { workspace_id, .. }
            | AddSessionToWorkspace { workspace_id, .. }
            | RemoveSessionFromWorkspace { workspace_id, .. } => Some(*workspace_id),
            _ => None,
        }
    }
}

/// Évènements renvoyés à l'utilisateur externe : les [`SessionEvent`]/
/// [`WorkspaceEvent`] bruts relayés depuis le réseau (uniquement pour les ids
/// autorisés, voir `MarieGatewayActor::handle_network_message`), les issues
/// des commandes d'accès, une réponse typée par commande, et deux variantes
/// génériques pour les refus d'autorisation (`CommandRejected`, rien n'a été
/// exécuté) et les échecs d'exécution (`CommandFailed`, la commande a
/// tourné mais a échoué côté `SessionClient`/`WorkspaceClient`). Pas
/// d'identifiant de corrélation : `MarieGatewayActor` traite une commande
/// entrante jusqu'au bout avant de revenir à sa boucle, l'ordre FIFO du
/// transport `foreign` suffit à faire correspondre une commande à sa
/// réponse.
#[derive(Debug, Serialize, Deserialize)]
pub enum MarieGatewayEvent {
    // --- Évènements de domaine relayés tels quels ---
    Session(SessionEvent),
    Workspace(WorkspaceEvent),

    // --- Issues des commandes d'accès ---
    SessionAccessGranted(SessionId),
    SessionAccessDenied(SessionId),
    WorkspaceAccessGranted(WorkspaceId),
    WorkspaceAccessDenied(WorkspaceId),

    // --- Réponses des commandes session ---
    SessionFetched(Session),
    SessionRemoved(SessionId),
    SessionLogAppended(SessionId),
    SessionVarsQueried { session_id: SessionId, values: Vec<Value> },
    SessionVarsPatched(SessionId),
    UserInputReported { session_id: SessionId, hitl_id: HitlFrameId },

    // --- Réponses des commandes workspace ---
    WorkspaceCreated(WorkspaceId),
    WorkspaceFetched(Workspace),
    WorkspaceRemoved(WorkspaceId),
    WorkspaceSessionsListed { workspace_id: WorkspaceId, sessions: Vec<SessionId> },
    WorkspaceVarsQueried { workspace_id: WorkspaceId, values: Vec<Value> },
    WorkspaceVarsPatched(WorkspaceId),
    SessionAddedToWorkspace { workspace_id: WorkspaceId, session_id: SessionId },
    SessionRemovedFromWorkspace { workspace_id: WorkspaceId, session_id: SessionId },
    SessionCreated { workspace_id: WorkspaceId, session_id: SessionId },

    // --- Génériques ---
    /// La commande n'a jamais été exécutée : l'id qu'elle porte n'a pas
    /// (encore, ou plus) été autorisé — voir `MarieGatewayActor::handle_command`.
    CommandRejected { reason: String },
    /// La commande a été exécutée mais a échoué côté `SessionClient`/
    /// `WorkspaceClient`, ou la vérification d'autorisation elle-même
    /// (`GatewayArgs::check_session`/`check_workspace`) a renvoyé une erreur.
    CommandFailed { reason: String },
}

#[cfg(test)]
mod tests {
    use marie_core::id::generate_id;

    use super::*;

    #[test]
    fn session_commands_require_prior_access() {
        let id = SessionId::new(generate_id());
        let cmd = MarieGatewayCommand::GetSession(id);
        assert_eq!(cmd.required_session(), Some(id));
        assert_eq!(cmd.required_workspace(), None);
    }

    #[test]
    fn workspace_commands_require_prior_access() {
        let id = WorkspaceId::new(generate_id());
        let cmd = MarieGatewayCommand::QueryWorkspaceVars { workspace_id: id, path: "$.foo".to_string() };
        assert_eq!(cmd.required_workspace(), Some(id));
        assert_eq!(cmd.required_session(), None);
    }

    #[test]
    fn access_and_creation_commands_require_nothing() {
        let session_id = SessionId::new(generate_id());
        let workspace_id = WorkspaceId::new(generate_id());

        assert_eq!(MarieGatewayCommand::AccessSession(session_id).required_session(), None);
        assert_eq!(MarieGatewayCommand::AccessWorkspace(workspace_id).required_workspace(), None);
        assert_eq!(MarieGatewayCommand::CreateWorkspace(workspace_id).required_workspace(), None);
        assert_eq!(MarieGatewayCommand::CreateSession(workspace_id).required_session(), None);
    }

    #[test]
    fn create_session_requires_workspace_access() {
        let workspace_id = WorkspaceId::new(generate_id());
        assert_eq!(MarieGatewayCommand::CreateSession(workspace_id).required_workspace(), Some(workspace_id));
    }
}
