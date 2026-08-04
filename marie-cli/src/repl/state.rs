use marie::{
    LocalMarie,
    expert::ExpertId,
    graph::{GraphSpec, NodeId, NodeSpec},
    session::SessionId,
};

pub struct CommandSpec {
    pub name: &'static str,
    pub usage: &'static str,
}

#[derive(Clone)]
pub enum SessionKind {
    Live(SessionId),
    Consult { expert_id: ExpertId, task: String },
}

pub enum ReplState {
    Root,
    LocalConnected { marie: LocalMarie },
    ExecutingSession { marie: LocalMarie, kind: SessionKind },
    EditingGraph { marie: LocalMarie, spec: GraphSpec },
    EditingNode { node_id: NodeId, spec: NodeSpec },
}

impl ReplState {
    /// Étiquette affichée dans l'invite (voir `repl::format_prompt`) — pas
    /// de `Display`/`fmt::Display` : ce n'est pas une conversion générale de
    /// l'état, seulement son rendu dans ce contexte précis.
    pub fn label(&self) -> String {
        match self {
            ReplState::Root => "root".to_string(),
            ReplState::LocalConnected { .. } => "local".to_string(),
            ReplState::ExecutingSession { kind: SessionKind::Live(id), .. } => format!("session:{id}"),
            ReplState::ExecutingSession { kind: SessionKind::Consult { expert_id, .. }, .. } => format!("consult:{expert_id}"),
            ReplState::EditingGraph { spec, .. } => format!("graph:{}", spec.id),
            // `NodeId` n'implémente pas `Display` côté marie-core (seulement
            // `Debug`) — rendu approximatif mais suffisant pour une invite.
            ReplState::EditingNode { node_id, .. } => format!("node:{node_id:?}"),
        }
    }

    pub fn commands(&self) -> &'static [CommandSpec] {
        match self {
            ReplState::Root => ROOT_COMMANDS,
            ReplState::LocalConnected { .. } => LOCAL_CONNECTED_COMMANDS,
            ReplState::ExecutingSession { .. } => EXECUTING_SESSION_COMMANDS,
            ReplState::EditingGraph { .. } => EDITING_GRAPH_COMMANDS,
            ReplState::EditingNode { .. } => EDITING_NODE_COMMANDS,
        }
    }
}

pub const ROOT_COMMANDS: &[CommandSpec] = &[
    CommandSpec { name: "connect local", usage: "connect local <in-memory|postgres>" },
    CommandSpec { name: "connect remote", usage: "connect remote <addr>  (pas encore implémenté)" },
    CommandSpec { name: "help", usage: "help" },
    CommandSpec { name: "exit", usage: "exit — quitte le REPL" },
];

pub const LOCAL_CONNECTED_COMMANDS: &[CommandSpec] = &[
    CommandSpec { name: "list sessions", usage: "list sessions" },
    CommandSpec { name: "create session shell", usage: "create session shell <expert-id>" },
    CommandSpec { name: "create session execute graph", usage: "create session execute graph <graph-id> [channel=value ...]" },
    CommandSpec { name: "create session consult expert", usage: "create session consult expert <expert-id> \"<task>\"" },
    CommandSpec { name: "delete session", usage: "delete session <id>" },
    CommandSpec { name: "list models", usage: "list models" },
    CommandSpec {
        name: "create model",
        usage: "create model <id> openai-compat base-url=<url> client-id=<id> api-key=<key> model=<model> [system-prompt=\"...\"]",
    },
    CommandSpec { name: "delete model", usage: "delete model <id>" },
    CommandSpec { name: "list experts", usage: "list experts" },
    CommandSpec {
        name: "create expert",
        usage: "create expert <id> model=<model-id> allowed-tools=<a,b,c> prompt=\"...\"",
    },
    CommandSpec { name: "delete expert", usage: "delete expert <id>" },
    CommandSpec { name: "set expert", usage: "set expert <id> [prompt=\"...\"]" },
    CommandSpec { name: "list tools", usage: "list tools" },
    CommandSpec { name: "list graphs", usage: "list graphs" },
    CommandSpec { name: "create graph", usage: "create graph <id>" },
    CommandSpec { name: "disconnect", usage: "disconnect" },
];

pub const EXECUTING_SESSION_COMMANDS: &[CommandSpec] = &[CommandSpec {
    name: "(aucune)",
    usage: "affiche les logs en direct et répond aux requêtes hitl — Ctrl-D pour se détacher",
}];

pub const EDITING_GRAPH_COMMANDS: &[CommandSpec] = &[
    CommandSpec { name: "set entry", usage: "set entry <node-id>" },
    CommandSpec { name: "list nodes", usage: "list nodes" },
    CommandSpec { name: "create node", usage: "create node <node-id> <kind>" },
    CommandSpec { name: "connect", usage: "connect <node-id-a> <node-id-b>" },
    CommandSpec { name: "disconnect", usage: "disconnect <node-id-a> <node-id-b>" },
    CommandSpec { name: "save", usage: "save — sauvegarde le graphe et quitte l'édition" },
];

pub const EDITING_NODE_COMMANDS: &[CommandSpec] = &[
    CommandSpec {
        name: "add channel",
        usage: "add channel <name> [exported=bool] [inherited=bool] [imported=bool] [reducer=append|lww|max] [default=<value>]",
    },
    CommandSpec {
        name: "set channel",
        usage: "set channel <name> [exported=bool] [inherited=bool] [imported=bool] [reducer=append|lww|max]",
    },
    CommandSpec { name: "save", usage: "save — enregistre le noeud dans le graphe en cours d'édition" },
];
