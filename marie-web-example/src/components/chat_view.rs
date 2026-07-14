//! Interface de "chat" de l'exemple : ouvre une session (existante ou
//! nouvellement créée), affiche ses frames/journal en direct (voir
//! `marie_leptos::{SessionFrames, SessionLogs}`) ainsi que les vars et
//! fichiers de la session et de son workspace. Affiche aussi les formulaires
//! HITL en attente (voir `marie_leptos::HitlForm`), tous agents/sessions
//! confondus — un formulaire n'est pas rattaché à la session actuellement
//! ouverte (voir [`crate::dto::HitlRequestDto::session_id`]).
//!
//! Compilé sous les deux features (`ssr`/`hydrate`, voir `crate::lib`) —
//! toute la logique ici ne s'exécute réellement qu'après hydratation
//! (`Effect::new`/`on:click`/`on:change` ne se déclenchent jamais côté
//! serveur en Leptos 0.8), donc rien à `#[cfg]` dans ce fichier lui-même.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use leptos::callback::UnsyncCallback;
use leptos::prelude::*;
use leptos::task::spawn_local;
use marie_leptos::types::{
    AgentStatusView, AnswerView, ContextEntryView, FrameView, HitlRequestView, QuestionKindView, QuestionView, RoleView, SessionLogSpecView,
    SessionLogView, SessionView, ToolCallView, YieldStatusView,
};
use marie_leptos::{HitlForm, SessionFrames, SessionLogs};

use crate::api;
use crate::dto::{
    self, AgentStatusDto, AnswerDto, ClientMessageDto, ContextEntryDto, FrameSnapshotDto, HitlRequestDto, QuestionDto, QuestionKindDto,
    RoleDto, ServerMessageDto, SessionEventDto, SessionLogDto, SessionLogSpecDto, ToolCallDto, VarsDto, YieldStatusDto,
};
use crate::hitl_upload;
use crate::ws_client::SessionSocket;

/// Résumé textuel d'un `mode` reçu en `Value` brute (voir la doc de
/// [`crate::dto::ServerMessageDto::Mode`]/`SessionEventDto::ModeChanged`
/// pour pourquoi ce n'est pas un type mirroré fidèlement) — juste assez pour
/// afficher le mode courant et décider si l'envoi de texte a un sens (voir
/// `ChatView`, qui ne l'active que pour `"simple"`/`"orchestration"`).
fn mode_label(mode: &serde_json::Value) -> String {
    match mode.get("mode").and_then(serde_json::Value::as_str) {
        Some("simple") => "simple".to_string(),
        Some("orchestration") => {
            let strategy = mode.get("strategy").and_then(serde_json::Value::as_str).unwrap_or("?");
            let children = mode.get("children").and_then(serde_json::Value::as_array).map_or(0, Vec::len);
            format!("orchestration (stratégie : {strategy}, {children} enfant(s))")
        }
        Some("state_graph") => {
            let current = mode.get("current").and_then(serde_json::Value::as_str).unwrap_or("?");
            format!("graphe d'états (nœud courant : {current})")
        }
        Some(other) => other.to_string(),
        None => "inconnu".to_string(),
    }
}

/// `mode` autorise-t-il l'envoi de texte pour démarrer un run (voir
/// [`crate::dto::ClientMessageDto::SendMessage`]) ? Seuls `"simple"` et
/// `"orchestration"` répondent à un message libre — `"state_graph"` suit un
/// graphe explicite, injecter du texte n'y a pas de sens (voir la doc de
/// `SendMessage`).
fn mode_accepts_message(mode: &serde_json::Value) -> bool {
    matches!(mode.get("mode").and_then(serde_json::Value::as_str), Some("simple") | Some("orchestration"))
}

fn role_view(role: RoleDto) -> RoleView {
    match role {
        RoleDto::System => RoleView::System,
        RoleDto::User => RoleView::User,
        RoleDto::Assistant => RoleView::Assistant,
        RoleDto::Tool => RoleView::Tool,
    }
}

fn yield_status_view(status: YieldStatusDto) -> YieldStatusView {
    match status {
        YieldStatusDto::WaitingToolReply { tool_call_id } => YieldStatusView::WaitingToolReply { tool_call_id },
        YieldStatusDto::WaitingChildren { children } => YieldStatusView::WaitingChildren { children },
        YieldStatusDto::RunExhausted => YieldStatusView::RunExhausted,
    }
}

fn agent_status_view(status: AgentStatusDto) -> AgentStatusView {
    match status {
        AgentStatusDto::Initial => AgentStatusView::Initial,
        AgentStatusDto::Paused => AgentStatusView::Paused,
        AgentStatusDto::Running => AgentStatusView::Running,
        AgentStatusDto::Failed => AgentStatusView::Failed,
        AgentStatusDto::Yielding(inner) => AgentStatusView::Yielding(yield_status_view(inner)),
        AgentStatusDto::Finished => AgentStatusView::Finished,
    }
}

fn context_entry_view(entry: ContextEntryDto) -> ContextEntryView {
    ContextEntryView { role: role_view(entry.role), content: entry.content }
}

fn frame_view(frame: FrameSnapshotDto) -> FrameView {
    FrameView {
        id: frame.id,
        model_id: frame.model_id,
        status: agent_status_view(frame.status),
        allowed_tools: frame.allowed_tools,
        context: frame.context.into_iter().map(context_entry_view).collect(),
        stdio: frame.stdio,
        stderr: frame.stderr,
    }
}

fn tool_call_view(call: ToolCallDto) -> ToolCallView {
    ToolCallView { name: call.name, parameters: call.parameters.map(|value| value.to_string()) }
}

fn session_log_view(log: SessionLogDto) -> SessionLogView {
    let data = match log.data {
        SessionLogSpecDto::AgentMessage { label, message } => SessionLogSpecView::AgentMessage { label, message },
        SessionLogSpecDto::ToolCall(call) => SessionLogSpecView::ToolCall(tool_call_view(call)),
    };
    SessionLogView { id: log.id, data }
}

fn question_kind_view(kind: QuestionKindDto) -> QuestionKindView {
    match kind {
        QuestionKindDto::ShortText => QuestionKindView::ShortText,
        QuestionKindDto::LongText => QuestionKindView::LongText,
        QuestionKindDto::Select { options } => QuestionKindView::Select { options },
        QuestionKindDto::Radio { options } => QuestionKindView::Radio { options },
        QuestionKindDto::Checkboxes { options } => QuestionKindView::Checkboxes { options },
        QuestionKindDto::FileUpload { accept } => QuestionKindView::FileUpload { accept },
    }
}

fn question_view(question: QuestionDto) -> QuestionView {
    QuestionView { key: question.key, label: question.label, kind: question_kind_view(question.kind) }
}

fn hitl_request_view(request: &HitlRequestDto) -> HitlRequestView {
    HitlRequestView {
        id: request.id.clone(),
        session_id: request.agent_id.0.clone(),
        local_id: request.agent_id.1.clone(),
        questions: request.questions.clone().into_iter().map(question_view).collect(),
    }
}

fn answer_dto(answer: AnswerView) -> AnswerDto {
    match answer {
        AnswerView::Single(value) => AnswerDto::Single(value),
        AnswerView::Multiple(values) => AnswerDto::Multiple(values),
    }
}

/// Connecte le websocket au premier appel (sans effet aux suivants, voir la
/// vérification `is_some()`) et branche `on_message` pour tenir `frames`/
/// `logs`/`session_vars` à jour en direct — une `FrameStatusChanged` ne porte
/// pas le frame complet, donc déclenche elle-même un `GetFrame` (voir
/// `crate::dto::ClientMessageDto::GetFrame`) pour aller le chercher.
fn ensure_socket(
    socket_cell: &Rc<RefCell<Option<SessionSocket>>>,
    status: RwSignal<String>,
    current_session: RwSignal<Option<String>>,
    frames: RwSignal<HashMap<String, FrameSnapshotDto>>,
    logs: RwSignal<Vec<SessionLogDto>>,
    session_vars: RwSignal<VarsDto>,
    pending_hitl: RwSignal<Vec<HitlRequestDto>>,
    current_mode: RwSignal<Option<serde_json::Value>>,
) {
    if socket_cell.borrow().is_some() {
        return;
    }

    let socket_cell_for_events = socket_cell.clone();
    let on_message = move |msg: ServerMessageDto| match msg {
        ServerMessageDto::Frame { frame: Some(frame), .. } => {
            frames.update(|map| {
                map.insert(frame.id.clone(), frame);
            });
        }
        ServerMessageDto::SessionEvent(SessionEventDto::FrameStatusChanged { session_id, local_id, .. }) => {
            if current_session.get_untracked().as_deref() == Some(session_id.as_str()) {
                let socket_cell = socket_cell_for_events.clone();
                spawn_local(async move {
                    // `take()` dans une instruction séparée, puis remise en
                    // place après l'attente, plutôt qu'un emprunt tenu à
                    // travers `.await` (le `RefMut` temporaire d'un
                    // `if let Some(..) = cell.borrow_mut()...` vit sinon
                    // jusqu'à la fin du bloc — voir
                    // `clippy::await_holding_refcell_ref` — ce qui risquerait
                    // de paniquer sur un envoi concurrent déclenché entre-temps
                    // par une autre tâche `spawn_local`).
                    let taken = socket_cell.borrow_mut().take();
                    if let Some(mut socket) = taken {
                        let _ = socket.send(&ClientMessageDto::GetFrame { session_id, local_id }).await;
                        *socket_cell.borrow_mut() = Some(socket);
                    }
                });
            }
        }
        ServerMessageDto::SessionEvent(SessionEventDto::LogAppended { session_id, log }) => {
            if current_session.get_untracked().as_deref() == Some(session_id.as_str()) {
                logs.update(|list| list.push(log));
            }
        }
        ServerMessageDto::SessionEvent(SessionEventDto::ValueChanged { session_id, key, value }) => {
            if current_session.get_untracked().as_deref() == Some(session_id.as_str()) {
                session_vars.update(|vars| {
                    vars.insert(key, value);
                });
            }
        }
        ServerMessageDto::SessionEvent(SessionEventDto::ValueRemoved { session_id, key })
            if current_session.get_untracked().as_deref() == Some(session_id.as_str()) =>
        {
            session_vars.update(|vars| {
                vars.remove(&key);
            });
        }
        ServerMessageDto::SessionEvent(SessionEventDto::ModeChanged { session_id, mode }) => {
            if current_session.get_untracked().as_deref() == Some(session_id.as_str()) {
                current_mode.set(Some(mode));
            }
        }
        ServerMessageDto::Mode { session_id, mode } => {
            if current_session.get_untracked().as_deref() == Some(session_id.as_str()) {
                current_mode.set(Some(mode));
            }
        }
        ServerMessageDto::HitlRequest(request) => {
            pending_hitl.update(|list| {
                if !list.iter().any(|existing| existing.id == request.id) {
                    list.push(request);
                }
            });
        }
        _ => {}
    };

    match SessionSocket::connect(on_message) {
        Ok(socket) => *socket_cell.borrow_mut() = Some(socket),
        Err(error) => status.set(format!("connexion websocket échouée : {error}")),
    }
}

#[component]
pub fn ChatView() -> impl IntoView {
    let session_id_input = RwSignal::new(String::new());
    let current_session = RwSignal::new(Option::<String>::None);
    let current_workspace = RwSignal::new(Option::<String>::None);
    let status = RwSignal::new(String::new());

    // Aucun registre cluster-wide des workspaces (voir la doc de
    // `marie_core::workspace::client::WorkspaceClient` — un workspace n'est
    // connu que de ses détenteurs, déduits de ses sessions membres) : cette
    // liste ne reflète que ce que cette page a créé/sélectionné depuis son
    // chargement, pas l'ensemble des workspaces du cluster.
    let workspaces = RwSignal::new(Vec::<String>::new());
    let selected_workspace = RwSignal::new(Option::<String>::None);

    let frames = RwSignal::new(HashMap::<String, FrameSnapshotDto>::new());
    let logs = RwSignal::new(Vec::<SessionLogDto>::new());
    let session_vars = RwSignal::new(VarsDto::new());
    let workspace_vars = RwSignal::new(VarsDto::new());
    let file_paths = RwSignal::new(Vec::<String>::new());

    // Mode actuellement au sommet de la pile de la session ouverte (voir
    // `mode_label`/`mode_accepts_message`) — `None` tant qu'aucune session
    // n'est ouverte ou que la réponse à `GetMode` n'est pas encore arrivée.
    let current_mode = RwSignal::new(Option::<serde_json::Value>::None);
    let message_models = RwSignal::new(Vec::<crate::dto::ModelDto>::new());
    let message_model_id = RwSignal::new(String::new());
    let message_allowed_tools = RwSignal::new(String::new());
    let message_text = RwSignal::new(String::new());

    let new_session_var_key = RwSignal::new(String::new());
    let new_session_var_value = RwSignal::new(String::new());
    let new_workspace_var_key = RwSignal::new(String::new());
    let new_workspace_var_value = RwSignal::new(String::new());
    let new_file_path = RwSignal::new(String::new());
    let selected_file = RwSignal::new(Option::<String>::None);
    let file_editor_content = RwSignal::new(String::new());

    let pending_hitl = RwSignal::new(Vec::<HitlRequestDto>::new());
    let hitl_answers = RwSignal::new(HashMap::<String, AnswerView>::new());

    let socket_cell: Rc<RefCell<Option<SessionSocket>>> = Rc::new(RefCell::new(None));

    // Connecté dès le montage plutôt qu'à la première ouverture de session :
    // les formulaires HITL sont globaux (voir la doc de module), pas
    // rattachés à une session particulière — inutile d'en ouvrir une pour
    // les recevoir. `Effect::new` ne se déclenche jamais côté serveur (voir
    // la doc de module), donc le websocket n'est ouvert qu'après hydratation.
    {
        let socket_cell = socket_cell.clone();
        Effect::new(move |_| ensure_socket(&socket_cell, status, current_session, frames, logs, session_vars, pending_hitl, current_mode));
    }

    // Catalogue de modèles pour le petit formulaire d'envoi de message (voir
    // la section "Envoyer" plus bas) — chargé une fois au montage, comme
    // dans `ConfigPanel`.
    Effect::new(move |_| {
        spawn_local(async move {
            if let Ok(list) = api::list_models().await {
                message_models.set(list);
            }
        });
    });

    let do_open = {
        let socket_cell = socket_cell.clone();
        move |session_id: String| {
            current_session.set(Some(session_id.clone()));
            current_workspace.set(None);
            frames.set(HashMap::new());
            logs.set(Vec::new());
            session_vars.set(VarsDto::new());
            workspace_vars.set(VarsDto::new());
            file_paths.set(Vec::new());
            current_mode.set(None);
            status.set(format!("ouverture de la session {session_id}…"));

            {
                let socket_cell = socket_cell.clone();
                let session_id = session_id.clone();
                spawn_local(async move {
                    // Voir le commentaire équivalent dans `ensure_socket`.
                    let taken = socket_cell.borrow_mut().take();
                    if let Some(mut socket) = taken {
                        let _ = socket.send(&ClientMessageDto::SubscribeSession { session_id: session_id.clone() }).await;
                        let _ = socket.send(&ClientMessageDto::GetMode { session_id }).await;
                        *socket_cell.borrow_mut() = Some(socket);
                    }
                });
            }

            {
                let session_id = session_id.clone();
                spawn_local(async move {
                    match api::session_vars(session_id.clone()).await {
                        Ok(vars) => session_vars.set(vars),
                        Err(error) => status.set(format!("échec de lecture des vars de session : {error}")),
                    }
                    match api::list_files(session_id.clone()).await {
                        Ok(list) => file_paths.set(list.paths),
                        Err(error) => status.set(format!("échec de la liste des fichiers : {error}")),
                    }
                    match api::session_workspace(session_id.clone()).await {
                        Ok(Some(workspace_id)) => {
                            current_workspace.set(Some(workspace_id.clone()));
                            if let Ok(vars) = api::workspace_vars(workspace_id).await {
                                workspace_vars.set(vars);
                            }
                        }
                        Ok(None) => current_workspace.set(None),
                        Err(error) => status.set(format!("échec de résolution du workspace : {error}")),
                    }
                    status.set(format!("session {session_id} ouverte"));
                });
            }
        }
    };

    let on_open_click = {
        let do_open = do_open.clone();
        move |_| {
            let session_id = session_id_input.get_untracked();
            if !session_id.is_empty() {
                do_open(session_id);
            }
        }
    };

    let on_new_session_click = move |_| {
        let do_open = do_open.clone();
        let workspace_id = selected_workspace.get_untracked();
        spawn_local(async move {
            match api::create_session(workspace_id).await {
                Ok(response) => {
                    workspaces.update(|list| {
                        if !list.contains(&response.workspace_id) {
                            list.push(response.workspace_id.clone());
                        }
                    });
                    selected_workspace.set(Some(response.workspace_id.clone()));
                    session_id_input.set(response.session_id.clone());
                    do_open(response.session_id);
                }
                Err(error) => status.set(format!("création de session échouée : {error}")),
            }
        });
    };

    let on_create_workspace_click = move |_| {
        spawn_local(async move {
            match api::create_workspace().await {
                Ok(workspace_id) => {
                    workspaces.update(|list| list.push(workspace_id.clone()));
                    selected_workspace.set(Some(workspace_id));
                }
                Err(error) => status.set(format!("échec de création du workspace : {error}")),
            }
        });
    };

    // Injecte `message_text` comme nouveau message utilisateur (voir
    // `crate::dto::ClientMessageDto::SendMessage`) — n'a de sens que pour un
    // mode `simple`/`orchestration` (voir `mode_accepts_message`), le bouton
    // "Envoyer" de la vue reste désactivé sinon. Le frame créé arrive par le
    // flux d'événements habituel (`SessionEvent::FrameStatusChanged`, déjà
    // géré par `ensure_socket`), rien à faire ici après l'envoi.
    let on_send_message = {
        let socket_cell = socket_cell.clone();
        move |_| {
            let Some(session_id) = current_session.get_untracked() else { return };
            let model_id = message_model_id.get_untracked();
            let text = message_text.get_untracked();
            if model_id.is_empty() || text.is_empty() {
                return;
            }
            let allowed_tools: Vec<String> =
                message_allowed_tools.get_untracked().split(',').map(str::trim).filter(|tool| !tool.is_empty()).map(str::to_string).collect();

            let socket_cell = socket_cell.clone();
            spawn_local(async move {
                let taken = socket_cell.borrow_mut().take();
                if let Some(mut socket) = taken {
                    let _ = socket.send(&ClientMessageDto::SendMessage { session_id, model_id, allowed_tools, text }).await;
                    *socket_cell.borrow_mut() = Some(socket);
                }
            });
            message_text.set(String::new());
        }
    };

    let on_add_session_var = move |_| {
        let Some(session_id) = current_session.get_untracked() else { return };
        let key = new_session_var_key.get_untracked();
        let raw_value = new_session_var_value.get_untracked();
        if key.is_empty() {
            return;
        }
        let value = serde_json::from_str(&raw_value).unwrap_or(serde_json::Value::String(raw_value));
        spawn_local(async move {
            if let Err(error) = api::set_session_var(session_id, key, value).await {
                status.set(format!("échec d'écriture de la var : {error}"));
            }
        });
        new_session_var_key.set(String::new());
        new_session_var_value.set(String::new());
    };

    let on_add_workspace_var = move |_| {
        let Some(workspace_id) = current_workspace.get_untracked() else { return };
        let key = new_workspace_var_key.get_untracked();
        let raw_value = new_workspace_var_value.get_untracked();
        if key.is_empty() {
            return;
        }
        let value = serde_json::from_str(&raw_value).unwrap_or(serde_json::Value::String(raw_value));
        spawn_local(async move {
            match api::set_workspace_var(workspace_id, key.clone(), value.clone()).await {
                Ok(()) => workspace_vars.update(|vars| {
                    vars.insert(key, value);
                }),
                Err(error) => status.set(format!("échec d'écriture de la var de workspace : {error}")),
            }
        });
        new_workspace_var_key.set(String::new());
        new_workspace_var_value.set(String::new());
    };

    let on_open_file = move |path: String| {
        let Some(session_id) = current_session.get_untracked() else { return };
        selected_file.set(Some(path.clone()));
        spawn_local(async move {
            match api::read_file(session_id, path).await {
                Ok(Some(content)) => file_editor_content.set(content.content),
                Ok(None) => file_editor_content.set(String::new()),
                Err(error) => status.set(format!("échec de lecture du fichier : {error}")),
            }
        });
    };

    let on_save_file = move |_| {
        let Some(session_id) = current_session.get_untracked() else { return };
        let Some(path) = selected_file.get_untracked() else { return };
        let content = file_editor_content.get_untracked();
        spawn_local(async move {
            match api::write_file(session_id.clone(), path, content).await {
                Ok(()) => {
                    if let Ok(list) = api::list_files(session_id).await {
                        file_paths.set(list.paths);
                    }
                }
                Err(error) => status.set(format!("échec d'écriture du fichier : {error}")),
            }
        });
    };

    let on_create_file = move |_| {
        let Some(session_id) = current_session.get_untracked() else { return };
        let path = new_file_path.get_untracked();
        if path.is_empty() {
            return;
        }
        spawn_local(async move {
            match api::write_file(session_id.clone(), path, String::new()).await {
                Ok(()) => {
                    if let Ok(list) = api::list_files(session_id).await {
                        file_paths.set(list.paths);
                    }
                }
                Err(error) => status.set(format!("échec de création du fichier : {error}")),
            }
        });
        new_file_path.set(String::new());
    };

    let on_hitl_file_selected = UnsyncCallback::new(move |(key, file): (String, web_sys::File)| {
        let Some(request) = pending_hitl.get_untracked().into_iter().next() else { return };
        let request_id = request.id.clone();
        let session_id = request.session_id().to_string();
        let filename = file.name();
        let blob = gloo_file::File::from(file);
        spawn_local(async move {
            let bytes = match gloo_file::futures::read_as_bytes(&blob).await {
                Ok(bytes) => bytes,
                Err(error) => {
                    status.set(format!("échec de lecture du fichier : {error}"));
                    return;
                }
            };
            let path = dto::hitl_upload_path(&request_id, &key, &filename);
            match hitl_upload::write_hitl_upload(&session_id, &path, &bytes).await {
                Ok(()) => hitl_answers.update(|map| {
                    map.insert(key, AnswerView::Single(filename));
                }),
                Err(error) => status.set(format!("échec de téléversement : {error}")),
            }
        });
    });

    let on_hitl_submit = UnsyncCallback::new(move |()| {
        let Some(request) = pending_hitl.get_untracked().into_iter().next() else { return };
        let request_id = request.id.clone();
        let answers: HashMap<String, AnswerDto> = hitl_answers.get_untracked().into_iter().map(|(key, value)| (key, answer_dto(value))).collect();
        let socket_cell = socket_cell.clone();
        {
            let request_id = request_id.clone();
            spawn_local(async move {
                let taken = socket_cell.borrow_mut().take();
                if let Some(mut socket) = taken {
                    let _ = socket.send(&ClientMessageDto::HitlAnswer { request_id, answers }).await;
                    *socket_cell.borrow_mut() = Some(socket);
                }
            });
        }
        pending_hitl.update(|list| list.retain(|pending| pending.id != request_id));
        hitl_answers.set(HashMap::new());
    });

    let session_view = Signal::derive(move || {
        let id = current_session.get().unwrap_or_default();
        let mut frame_list: Vec<FrameView> = frames.get().into_values().map(frame_view).collect();
        frame_list.sort_by(|a, b| a.id.cmp(&b.id));
        let log_list: Vec<SessionLogView> = logs.get().into_iter().map(session_log_view).collect();
        SessionView { id, frames: frame_list, logs: log_list }
    });

    view! {
        <div class="chat-view">
            <section class="panel">
                <h2>"Workspaces"</h2>
                <ul>
                    <For each=move || workspaces.get() key=|workspace_id| workspace_id.clone() let:workspace_id>
                        {
                            let marker_id = workspace_id.clone();
                            let select_id = workspace_id.clone();
                            let delete_id = workspace_id.clone();
                            view! {
                                <li>
                                    <button class="link" on:click=move |_| selected_workspace.set(Some(select_id.clone()))>
                                        {move || if selected_workspace.get().as_deref() == Some(marker_id.as_str()) { "▶ " } else { "" }}
                                        {workspace_id.clone()}
                                    </button>
                                    <button class="link" on:click=move |_| {
                                        let delete_id = delete_id.clone();
                                        spawn_local(async move {
                                            if let Err(error) = api::delete_workspace(delete_id.clone()).await {
                                                status.set(format!("échec de suppression du workspace : {error}"));
                                            }
                                            workspaces.update(|list| list.retain(|id| id != &delete_id));
                                            if selected_workspace.get_untracked().as_deref() == Some(delete_id.as_str()) {
                                                selected_workspace.set(None);
                                            }
                                        });
                                    }>"Supprimer"</button>
                                </li>
                            }
                        }
                    </For>
                </ul>
                <p class="hint">"Sélectionné : " {move || selected_workspace.get().unwrap_or_else(|| "(aucun — nouveau workspace à la création)".to_string())}</p>
                <button on:click=on_create_workspace_click>"Créer un workspace"</button>
            </section>

            <section class="panel">
                <h2>"Session"</h2>
                <div class="row">
                    <input placeholder="identifiant de session (32 caractères hex)" prop:value=move || session_id_input.get()
                        on:input=move |ev| session_id_input.set(event_target_value(&ev)) />
                    <button on:click=on_open_click>"Ouvrir"</button>
                    <button on:click=on_new_session_click>"Nouvelle session"</button>
                </div>
                <p class="status">{move || status.get()}</p>
                <p class="hint">
                    "Workspace : " {move || current_workspace.get().unwrap_or_else(|| "—".to_string())}
                    " — \"Nouvelle session\" l'ouvre dans le workspace sélectionné ci-dessus (ou en crée un nouveau si aucun n'est sélectionné)."
                </p>
            </section>

            // Pas de `<Show>` autour de ce panneau/formulaire (contrairement
            // aux autres sections conditionnelles de cette vue) : `Show`
            // exige que son contenu implémente `Send + Sync` (voir
            // `leptos::prelude::ToChildren`), ce que `on_send_message` ne
            // satisfait pas — il capture `socket_cell` (`Rc<RefCell<...>>`,
            // jamais `Send`, comme partout ailleurs dans ce fichier). La
            // visibilité est donc pilotée par `style:display` plutôt que par
            // un montage/démontage conditionnel du sous-arbre.
            <section class="panel" style:display=move || if current_session.get().is_some() { "block" } else { "none" }>
                <h2>"Mode"</h2>
                <p class="hint">{move || current_mode.get().as_ref().map_or_else(|| "chargement…".to_string(), mode_label)}</p>

                <div class="form-grid" style:display=move || if current_mode.get().as_ref().is_some_and(mode_accepts_message) { "grid" } else { "none" }>
                    <select prop:value=move || message_model_id.get() on:change=move |ev| message_model_id.set(event_target_value(&ev))>
                        <option value="">"(choisir un modèle)"</option>
                        <For each=move || message_models.get() key=|model| model.id.clone() let:model>
                            <option value=model.id.clone()>{model.id.clone()}" — "{model.model.clone()}</option>
                        </For>
                    </select>
                    <input placeholder="allowed_tools (séparés par des virgules, optionnel)" prop:value=move || message_allowed_tools.get()
                        on:input=move |ev| message_allowed_tools.set(event_target_value(&ev)) />
                    <textarea rows="3" placeholder="message" prop:value=move || message_text.get()
                        on:input=move |ev| message_text.set(event_target_value(&ev)) />
                    <button on:click=on_send_message>"Envoyer"</button>
                </div>
                <p class="hint" style:display=move || if current_mode.get().as_ref().is_some_and(mode_accepts_message) { "none" } else { "block" }>
                    "L'envoi de texte n'est disponible qu'en mode simple ou orchestrateur (pas en graphe d'états)."
                </p>
            </section>

            <Show when=move || !pending_hitl.get().is_empty()>
                <section class="panel hitl-panel">
                    <h2>"Formulaire humain en attente"</h2>
                    <p class="hint">
                        "Tous agents/sessions confondus — "
                        {move || pending_hitl.get().len()}" en attente."
                    </p>
                    {move || {
                        pending_hitl.get().first().map(|request| {
                            let view = hitl_request_view(request);
                            view! {
                                <p class="hint">"Session : "{view.session_id.clone()}" — agent : "{view.local_id.clone()}</p>
                                <HitlForm request=view answers=hitl_answers on_file_selected=on_hitl_file_selected on_submit=on_hitl_submit />
                            }
                        })
                    }}
                </section>
            </Show>

            <section class="panel">
                <h2>"Frames"</h2>
                <SessionFrames session=session_view render_frame=Callback::new(move |frame: FrameView| {
                    view! {
                        <div class="frame">
                            <div class="frame-header">
                                <strong>{frame.id.clone()}</strong>
                                <span class="badge">{frame.status.label()}</span>
                            </div>
                            <div class="frame-context">
                                <For each=move || frame.context.clone() key=|entry| entry.content.clone() let:entry>
                                    <p class="context-entry"><em>{entry.role.label()}</em>": "{entry.content}</p>
                                </For>
                            </div>
                            <pre class="stdio">{frame.stdio.clone()}</pre>
                        </div>
                    }.into_any()
                }) />
            </section>

            <section class="panel">
                <h2>"Journal"</h2>
                <SessionLogs session=session_view render_log=Callback::new(move |log: SessionLogView| {
                    view! {
                        <p class="log-entry">
                            {match log.data {
                                SessionLogSpecView::AgentMessage { label, message } => format!("[{label}] {message}"),
                                SessionLogSpecView::ToolCall(call) => format!("tool: {} {}", call.name, call.parameters.unwrap_or_default()),
                            }}
                        </p>
                    }.into_any()
                }) />
            </section>

            <section class="panel">
                <h2>"Vars de session"</h2>
                <ul class="var-list">
                    <For each=move || { session_vars.get().into_iter().collect::<Vec<_>>() } key=|(key, _)| key.clone() let:entry>
                        <li>{entry.0.clone()}" = "{entry.1.to_string()}</li>
                    </For>
                </ul>
                <div class="row">
                    <input placeholder="clé" prop:value=move || new_session_var_key.get() on:input=move |ev| new_session_var_key.set(event_target_value(&ev)) />
                    <input placeholder="valeur (JSON ou texte)" prop:value=move || new_session_var_value.get() on:input=move |ev| new_session_var_value.set(event_target_value(&ev)) />
                    <button on:click=on_add_session_var>"Définir"</button>
                </div>
            </section>

            <section class="panel">
                <h2>"Vars de workspace"</h2>
                <ul class="var-list">
                    <For each=move || { workspace_vars.get().into_iter().collect::<Vec<_>>() } key=|(key, _)| key.clone() let:entry>
                        <li>{entry.0.clone()}" = "{entry.1.to_string()}</li>
                    </For>
                </ul>
                <div class="row">
                    <input placeholder="clé" prop:value=move || new_workspace_var_key.get() on:input=move |ev| new_workspace_var_key.set(event_target_value(&ev)) />
                    <input placeholder="valeur (JSON ou texte)" prop:value=move || new_workspace_var_value.get() on:input=move |ev| new_workspace_var_value.set(event_target_value(&ev)) />
                    <button on:click=on_add_workspace_var>"Définir"</button>
                </div>
            </section>

            <section class="panel">
                <h2>"Fichiers de session"</h2>
                <ul class="file-list">
                    <For each=move || file_paths.get() key=|path| path.clone() let:path>
                        {
                            let open_path = path.clone();
                            view! { <li><button class="link" on:click=move |_| on_open_file(open_path.clone())>{path}</button></li> }
                        }
                    </For>
                </ul>
                <div class="row">
                    <input placeholder="nouveau fichier (ex: notes.md)" prop:value=move || new_file_path.get() on:input=move |ev| new_file_path.set(event_target_value(&ev)) />
                    <button on:click=on_create_file>"Créer"</button>
                </div>
                <Show when=move || selected_file.get().is_some()>
                    <div class="file-editor">
                        <p>{move || selected_file.get().unwrap_or_default()}</p>
                        <textarea rows="10" prop:value=move || file_editor_content.get()
                            on:input=move |ev| file_editor_content.set(event_target_value(&ev)) />
                        <button on:click=on_save_file>"Enregistrer"</button>
                    </div>
                </Show>
            </section>
        </div>
    }
}
