use marie::{
    expert::Expert,
    graph::GraphSpec,
    hitl::{Question, QuestionKind},
    model::EncryptedModel,
    session::{Session, logs::SessionLog, logs::SessionLogContent},
    tools::Tool,
};

pub fn format_sessions(sessions: &[Session]) -> String {
    if sessions.is_empty() {
        return "(aucune session)".to_string();
    }
    sessions
        .iter()
        .map(|s| format!("{}  status={:?}  créée={}", s.id, s.status, s.created_at))
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn format_models(models: &[EncryptedModel]) -> String {
    if models.is_empty() {
        return "(aucun modèle)".to_string();
    }
    models
        .iter()
        .map(|m| match m {
            EncryptedModel::OpenAICompatible { id, base_url, client_id, model, system_prompt, .. } => format!(
                "{id}  base-url={base_url}  client-id={client_id}  model={model}  system-prompt={}",
                system_prompt.as_deref().unwrap_or("(aucun)")
            ),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn format_experts(experts: &[Expert]) -> String {
    if experts.is_empty() {
        return "(aucun expert)".to_string();
    }
    experts
        .iter()
        .map(|e| {
            let tools = e.allowed_tools.iter().map(|t| t.to_string()).collect::<Vec<_>>().join(",");
            format!("{}  model={}  allowed-tools=[{tools}]  prompt={:?}", e.id, e.model_id, e.prompt)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn format_tools(tools: &[Tool]) -> String {
    if tools.is_empty() {
        return "(aucun outil)".to_string();
    }
    tools
        .iter()
        .map(|t| format!("{}  {}", t.name, t.description))
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn format_graphs(graphs: &[GraphSpec]) -> String {
    if graphs.is_empty() {
        return "(aucun graphe)".to_string();
    }
    graphs
        .iter()
        .map(|g| format!("{}  entry={:?}  nodes={}", g.id, g.entry, g.nodes.len()))
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn format_nodes(spec: &GraphSpec) -> String {
    if spec.nodes.is_empty() {
        return "(aucun noeud)".to_string();
    }
    spec.nodes
        .iter()
        .map(|(id, node)| {
            let channels = node.common.channels.iter().map(|c| c.name().to_string()).collect::<Vec<_>>().join(",");
            format!("{id:?}  kind={:?}  channels=[{channels}]", node.kind)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn format_log(log: &SessionLog) -> String {
    match &log.content {
        SessionLogContent::Plain(text) => text.clone(),
        SessionLogContent::ToolLog { label, input, output } => format!("[tool:{label}] in={input} out={output}"),
        SessionLogContent::AgentLog { label, content } => format!("[{label}] {content}"),
    }
}

pub fn format_question(q: &Question) -> String {
    match &q.kind {
        QuestionKind::ShortText => format!("{} (texte court)", q.label),
        QuestionKind::LongText => format!("{} (texte long, terminez par '.' seul sur une ligne)", q.label),
        QuestionKind::Select { options } => format!("{} — choisissez un: {}", q.label, format_options(options)),
        QuestionKind::Radio { options } => format!("{} — choisissez un: {}", q.label, format_options(options)),
        QuestionKind::Checkboxes { options } => format!("{} — choisissez zéro ou plusieurs (virgules): {}", q.label, format_options(options)),
        QuestionKind::FileUpload { accept } => format!("{} — chemin de fichier (types acceptés: {})", q.label, accept.join(",")),
    }
}

fn format_options(options: &[String]) -> String {
    options
        .iter()
        .enumerate()
        .map(|(i, o)| format!("{}) {o}", i + 1))
        .collect::<Vec<_>>()
        .join("  ")
}
