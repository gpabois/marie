pub mod protocol;

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::{Sink, SinkExt, Stream, StreamExt};
use marie_core::{
    client::Client,
    layer::Layer,
    pubsub::{PubSubMessage, layers::PubSubLayer},
    session::{Session, SessionEvent, SessionId, client::SessionClient},
    workspace::{WorkspaceEvent, WorkspaceId, client::WorkspaceClient},
};
use tokio::sync::mpsc;

/// Prédicats d'autorisation appelés une seule fois par id, au moment de la
/// commande [`protocol::MarieGatewayCommand::AccessSession`]/
/// `AccessWorkspace` correspondante — `Ok(true)` est ce qui fait entrer l'id
/// dans les ensembles autorisés de `MarieGatewayActor` (voir
/// `MarieGatewayActor::handle_command`). `Send + Sync` est requis : l'acteur
/// est déplacé dans `tokio::spawn`, dont le futur doit être `Send + 'static`.
pub struct GatewayArgs {
    check_session: Arc<dyn Fn(SessionId) -> BoxFuture<'static, anyhow::Result<bool>> + Send + Sync>,
    check_workspace: Arc<dyn Fn(WorkspaceId) -> BoxFuture<'static, anyhow::Result<bool>> + Send + Sync>,
}

impl GatewayArgs {
    #[must_use]
    pub fn new(
        check_session: impl Fn(SessionId) -> BoxFuture<'static, anyhow::Result<bool>> + Send + Sync + 'static,
        check_workspace: impl Fn(WorkspaceId) -> BoxFuture<'static, anyhow::Result<bool>> + Send + Sync + 'static,
    ) -> Self {
        Self { check_session: Arc::new(check_session), check_workspace: Arc::new(check_workspace) }
    }
}

/// Commande interne de contrôle de l'acteur — distincte de
/// `protocol::MarieGatewayCommand` (qui vient de `foreign`) : seule
/// `Shutdown` existe, déclenchée par `Handle::drop`/`MarieGateway::shutdown`.
enum GatewayControl {
    Shutdown,
}

/// Réplique à l'identique de `network::actor::Handle`/`rpc::client::RpcClientInner` :
/// tant qu'au moins un clone de [`MarieGateway`] existe, `Arc<Handle>` a un
/// `strong_count` > 0 ; au dernier `drop`, `Handle::drop` envoie
/// `GatewayControl::Shutdown`, que `MarieGatewayActor::run` traite en
/// sortant de sa boucle. Un déplacement (`move`) d'un `MarieGateway` ne
/// déclenche jamais ce `Drop` — sémantique standard de Rust (`Drop` ne
/// s'exécute que quand la portée propriétaire d'une valeur se termine sans
/// que la valeur en ait été déplacée), rien de spécifique à coder ici.
struct Handle(mpsc::UnboundedSender<GatewayControl>);

impl Drop for Handle {
    fn drop(&mut self) {
        let _ = self.0.send(GatewayControl::Shutdown);
    }
}

/// Poignée `Clone` de [`MarieGatewayActor`]. Ne porte aucune autre donnée :
/// toute la circulation de commandes/évènements passe par `foreign` (fourni
/// à [`MarieGatewayActor::create`]), pas par ce handle — son seul rôle est
/// de garder l'acteur en vie et de permettre un arrêt explicite.
#[derive(Clone)]
pub struct MarieGateway {
    commands: mpsc::UnboundedSender<GatewayControl>,
    handle: Arc<Handle>,
}

impl MarieGateway {
    /// Arrête explicitement l'acteur sans attendre un `Drop` — idempotent.
    pub fn shutdown(&self) {
        let _ = self.commands.send(GatewayControl::Shutdown);
    }
}

pub struct MarieGatewayActor {
    // Conservé uniquement pour que `commands_rx` ne se ferme jamais tout
    // seul — l'arrêt est exclusivement piloté par `GatewayControl::Shutdown`
    // (même choix que `network::actor::NetworkActor::commands_tx`).
    commands_tx: mpsc::UnboundedSender<GatewayControl>,
    commands_rx: mpsc::UnboundedReceiver<GatewayControl>,

    // Transport externe — boxé + épinglé pour ne pas dépendre de l'`Unpin`
    // du `Layer::Sender`/`Layer::Receiver` concret fourni par l'appelant
    // (même idiome que `RpcClientActor::run`, `Box::pin(tx)`/`Box::pin(rx)`).
    foreign_tx: Pin<Box<dyn Sink<protocol::MarieGatewayEvent, Error = anyhow::Error> + Send>>,
    foreign_rx: Pin<Box<dyn Stream<Item = protocol::MarieGatewayCommand> + Send>>,

    // Flux réseau déjà décodé en `PubSubMessage` (via `PubSubLayer`), déjà
    // `BoxStream` donc déjà `Pin<Box<...>>`/`Unpin`.
    network_rx: futures::stream::BoxStream<'static, PubSubMessage>,

    sessions: SessionClient,
    workspaces: WorkspaceClient,

    check_session: Arc<dyn Fn(SessionId) -> BoxFuture<'static, anyhow::Result<bool>> + Send + Sync>,
    check_workspace: Arc<dyn Fn(WorkspaceId) -> BoxFuture<'static, anyhow::Result<bool>> + Send + Sync>,

    authorized_sessions: HashSet<SessionId>,
    authorized_workspaces: HashSet<WorkspaceId>,
}

impl MarieGatewayActor {
    /// Construit une passerelle pour UNE connexion externe (ex. une socket
    /// websocket ouverte par `marie-ws`) : `foreign` porte les
    /// `MarieGatewayCommand`/`MarieGatewayEvent` de cette connexion, `client`
    /// donne accès au réseau (abonnement gossipsub) et aux clients de
    /// sessions/workspaces déjà connectés au cluster. L'autorisation
    /// (`args`) est propre à cette instance : deux connexions distinctes
    /// pour le même utilisateur ont chacune leur propre acteur et doivent
    /// chacune rejouer `AccessSession`/`AccessWorkspace`.
    #[must_use]
    pub fn create(
        client: Client,
        args: GatewayArgs,
        foreign: impl Layer<Send = protocol::MarieGatewayEvent, Received = protocol::MarieGatewayCommand>,
    ) -> MarieGateway {
        let network = client.network();

        // On construit le PubSubLayer (donc on s'abonne au
        // `broadcast::Receiver` de `Network`) AVANT de demander les
        // abonnements gossipsub eux-mêmes, pour ne rater aucun
        // `PubSubReceived` émis entre les deux.
        let pubsub = PubSubLayer::new(network.transport());
        let (_pubsub_tx, network_rx) = pubsub.split();

        // Abonnements effectifs : un topic global par type d'évènement,
        // sessions ET workspaces. On préfère les topics globaux aux topics
        // dédiés par session/workspace : `NetworkCommand` n'a pas de variante
        // `Unsubscribe` aujourd'hui, donc des abonnements dédiés créés au
        // fil des `AccessSession`/`AccessWorkspace` s'accumuleraient sans
        // jamais être nettoyés, pour un processus passerelle de longue
        // durée servant de nombreuses connexions. Les topics globaux ont un
        // coût fixe et borné (14 abonnements), au prix de recevoir (puis
        // filtrer côté client, voir `handle_network_message`) le trafic de
        // TOUT le cluster plutôt que des seuls ids autorisés — ça
        // fonctionne car `SessionEventLayer`/`WorkspaceEventLayer` publient
        // toujours à la fois sur le topic dédié et sur le topic global.
        for topic in SessionEvent::all_global_topics() {
            network.subscribe(topic);
        }
        for topic in WorkspaceEvent::all_global_topics() {
            network.subscribe(topic);
        }

        let (foreign_tx, foreign_rx) = foreign.split();
        let foreign_tx: Pin<Box<dyn Sink<protocol::MarieGatewayEvent, Error = anyhow::Error> + Send>> = Box::pin(foreign_tx);
        let foreign_rx: Pin<Box<dyn Stream<Item = protocol::MarieGatewayCommand> + Send>> = Box::pin(foreign_rx);

        let (commands_tx, commands_rx) = mpsc::unbounded_channel::<GatewayControl>();

        let actor = MarieGatewayActor {
            commands_tx: commands_tx.clone(),
            commands_rx,
            foreign_tx,
            foreign_rx,
            network_rx,
            sessions: client.sessions.clone(),
            workspaces: client.workspaces.clone(),
            check_session: args.check_session,
            check_workspace: args.check_workspace,
            authorized_sessions: HashSet::new(),
            authorized_workspaces: HashSet::new(),
        };

        tokio::spawn(actor.run());

        MarieGateway { commands: commands_tx.clone(), handle: Arc::new(Handle(commands_tx)) }
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                Some(control) = self.commands_rx.recv() => {
                    match control {
                        GatewayControl::Shutdown => break,
                    }
                }
                command = self.foreign_rx.next() => {
                    match command {
                        Some(command) => self.handle_command(command).await,
                        // Connexion externe fermée (ex. websocket) : plus
                        // rien à relayer à cet utilisateur. On arrête
                        // explicitement (`break`) plutôt que de laisser ce
                        // bras cesser silencieusement de correspondre :
                        // `tokio::select!` ne retombe pas sur un `else`
                        // quand un motif ne correspond plus, un `Stream`
                        // terminé continuerait donc à réveiller la boucle
                        // sans jamais l'arrêter si on ne gérait pas `None`
                        // explicitement.
                        None => break,
                    }
                }
                Some(msg) = self.network_rx.next() => {
                    self.handle_network_message(msg).await;
                }
            }
        }
    }

    async fn handle_command(&mut self, command: protocol::MarieGatewayCommand) {
        use protocol::{MarieGatewayCommand as C, MarieGatewayEvent as E};

        let reply = match command {
            C::AccessSession(id) => match (self.check_session)(id).await {
                Ok(true) => {
                    self.authorized_sessions.insert(id);
                    E::SessionAccessGranted(id)
                }
                Ok(false) => E::SessionAccessDenied(id),
                Err(err) => E::CommandFailed { reason: err.to_string() },
            },
            C::AccessWorkspace(id) => match (self.check_workspace)(id).await {
                Ok(true) => {
                    self.authorized_workspaces.insert(id);
                    E::WorkspaceAccessGranted(id)
                }
                Ok(false) => E::WorkspaceAccessDenied(id),
                Err(err) => E::CommandFailed { reason: err.to_string() },
            },
            other => {
                // Vérification d'autorisation AVANT tout dispatch — c'est
                // ici que "ne jamais laisser passer" est appliqué pour les
                // commandes : si l'id requis n'est pas dans l'ensemble
                // autorisé, on ne touche JAMAIS `sessions`/`workspaces`.
                let session_ok = match other.required_session() {
                    Some(id) => self.authorized_sessions.contains(&id),
                    None => true,
                };
                let workspace_ok = match other.required_workspace() {
                    Some(id) => self.authorized_workspaces.contains(&id),
                    None => true,
                };

                if !session_ok || !workspace_ok {
                    E::CommandRejected { reason: "accès non autorisé pour cette session/workspace".to_string() }
                } else {
                    self.dispatch(other).await
                }
            }
        };

        let _ = self.foreign_tx.send(reply).await;
    }

    async fn dispatch(&mut self, command: protocol::MarieGatewayCommand) -> protocol::MarieGatewayEvent {
        use protocol::{MarieGatewayCommand as C, MarieGatewayEvent as E};

        match command {
            C::AccessSession(_) | C::AccessWorkspace(_) => unreachable!("géré par handle_command"),

            C::GetSession(id) => match self.sessions.get(id).await {
                Ok(session) => E::SessionFetched(session),
                Err(err) => E::CommandFailed { reason: err.to_string() },
            },
            C::RemoveSession(id) => match self.sessions.remove(id).await {
                Ok(()) => E::SessionRemoved(id),
                Err(err) => E::CommandFailed { reason: err.to_string() },
            },
            C::AppendSessionLog { session_id, line } => match self.sessions.append_log(session_id, line).await {
                Ok(()) => E::SessionLogAppended(session_id),
                Err(err) => E::CommandFailed { reason: err.to_string() },
            },
            C::QuerySessionVars { session_id, path } => match self.sessions.query_state(session_id, path).await {
                Ok(values) => E::SessionVarsQueried { session_id, values },
                Err(err) => E::CommandFailed { reason: err.to_string() },
            },
            C::PatchSessionVars { session_id, path, value } => match self.sessions.patch_vars(session_id, path, value).await {
                Ok(()) => E::SessionVarsPatched(session_id),
                Err(err) => E::CommandFailed { reason: err.to_string() },
            },
            C::ReportUserInput { session_id, hitl_id, answers } => match self.sessions.report_user_input(session_id, hitl_id, answers).await {
                Ok(resolved) => E::UserInputReported { session_id, hitl_id: resolved },
                Err(err) => E::CommandFailed { reason: err.to_string() },
            },

            C::CreateWorkspace(id) => match self.workspaces.create(id).await {
                Ok(()) => {
                    // Le créateur d'un workspace y est autorisé d'office :
                    // il n'a pas pu être autorisé au préalable puisque le
                    // workspace n'existait pas encore.
                    self.authorized_workspaces.insert(id);
                    E::WorkspaceCreated(id)
                }
                Err(err) => E::CommandFailed { reason: err.to_string() },
            },
            C::GetWorkspace(id) => match self.workspaces.get(id).await {
                Ok(workspace) => E::WorkspaceFetched(workspace),
                Err(err) => E::CommandFailed { reason: err.to_string() },
            },
            C::RemoveWorkspace(id) => match self.workspaces.remove(id).await {
                Ok(()) => E::WorkspaceRemoved(id),
                Err(err) => E::CommandFailed { reason: err.to_string() },
            },
            C::ListWorkspaceSessions(id) => match self.workspaces.sessions(id).await {
                Ok(sessions) => E::WorkspaceSessionsListed { workspace_id: id, sessions },
                Err(err) => E::CommandFailed { reason: err.to_string() },
            },
            C::QueryWorkspaceVars { workspace_id, path } => match self.workspaces.query_vars(workspace_id, path).await {
                Ok(values) => E::WorkspaceVarsQueried { workspace_id, values },
                Err(err) => E::CommandFailed { reason: err.to_string() },
            },
            C::PatchWorkspaceVars { workspace_id, path, value } => match self.workspaces.patch_vars(workspace_id, path, value).await {
                Ok(()) => E::WorkspaceVarsPatched(workspace_id),
                Err(err) => E::CommandFailed { reason: err.to_string() },
            },
            C::AddSessionToWorkspace { workspace_id, session_id } => match self.workspaces.add_session(workspace_id, session_id).await {
                Ok(()) => E::SessionAddedToWorkspace { workspace_id, session_id },
                Err(err) => E::CommandFailed { reason: err.to_string() },
            },
            C::RemoveSessionFromWorkspace { workspace_id, session_id } => match self.workspaces.remove_session(workspace_id, session_id).await {
                Ok(()) => E::SessionRemovedFromWorkspace { workspace_id, session_id },
                Err(err) => E::CommandFailed { reason: err.to_string() },
            },
            C::CreateSession(workspace_id) => self.handle_create_session(workspace_id).await,
        }
    }

    /// Enchaîne `WorkspaceClient::create_session` (génère un `SessionId`,
    /// rattache au workspace) puis `SessionClient::insert` d'une `Session`
    /// vide (pas de `Session::new()`). Si le second appel échoue, on tente
    /// un rattrapage best-effort en détachant l'id désormais orphelin du
    /// workspace (`remove_session`) ; si CE rattrapage échoue aussi, le
    /// workspace garde une référence à un `session_id` sans enregistrement
    /// — cas résiduel rare, accepté (pas de transaction inter-catalogues
    /// possible ici, ce sont deux RPC indépendantes). La session créée est
    /// autorisée d'office pour cette connexion : son créateur n'a pas pu
    /// l'autoriser au préalable puisqu'elle n'existait pas encore.
    async fn handle_create_session(&mut self, workspace_id: WorkspaceId) -> protocol::MarieGatewayEvent {
        use protocol::MarieGatewayEvent as E;

        let session_id = match self.workspaces.create_session(workspace_id).await {
            Ok(id) => id,
            Err(err) => return E::CommandFailed { reason: format!("échec de rattachement au workspace : {err}") },
        };

        let empty = Session {
            id: session_id,
            frames: Default::default(),
            graphs: Default::default(),
            orchestrations: Default::default(),
            hitls: Default::default(),
            logs: Vec::new(),
            vars: Default::default(),
            created_at: chrono::Utc::now(), // écrasé par le store à l'insert
            last_updated_at: chrono::Utc::now(),
        };

        match self.sessions.insert(empty).await {
            Ok(()) => {
                self.authorized_sessions.insert(session_id);
                E::SessionCreated { workspace_id, session_id }
            }
            Err(err) => {
                let _ = self.workspaces.remove_session(workspace_id, session_id).await;
                E::CommandFailed { reason: format!("échec de création de session : {err}") }
            }
        }
    }

    /// Filtre les évènements réseau : jamais transmis si l'id concerné
    /// n'est pas dans `authorized_sessions`/`authorized_workspaces` — c'est
    /// ici que "ne jamais laisser passer" s'applique côté évènements.
    async fn handle_network_message(&mut self, msg: PubSubMessage) {
        use protocol::MarieGatewayEvent as E;

        if SessionEvent::is(&msg) {
            if let Ok(event) = SessionEvent::try_from(msg) {
                if self.authorized_sessions.contains(&event.session_id()) {
                    let _ = self.foreign_tx.send(E::Session(event)).await;
                }
                // sinon : abandonné silencieusement, jamais transmis.
            }
            return;
        }

        if WorkspaceEvent::is(&msg) {
            if let Ok(event) = WorkspaceEvent::try_from(msg) {
                if self.authorized_workspaces.contains(&event.workspace_id()) {
                    let _ = self.foreign_tx.send(E::Workspace(event)).await;
                }
            }
        }
        // Ni l'un ni l'autre : ne devrait pas arriver vu les abonnements
        // exacts faits au démarrage — ignoré par prudence.
    }
}
