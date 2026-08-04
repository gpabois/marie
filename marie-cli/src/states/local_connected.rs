use std::collections::HashMap;

use marie::{
    LocalMarie,
    expert::{Expert, ExpertId},
    graph::{GraphId, GraphSpec, NodeId},
    model::{CreateModel, ModelId},
    session::{SessionId, channel::ChannelName, protocol::NewSessionArgs, spec::CommonSpec},
    shell::ShellMode,
    tools::ToolId,
};

use crate::repl::{
    Repl,
    args::{parse_command_args, parse_value},
    error::CliError,
    render,
    state::{ReplState, SessionKind},
};

pub async fn dispatch(repl: &mut Repl, tokens: &[String]) -> Result<(), CliError> {
    let marie = {
        let ReplState::LocalConnected { marie } = repl.top() else { unreachable!() };
        marie.clone()
    };

    match (tokens.first().map(String::as_str), tokens.get(1).map(String::as_str)) {
        (Some("list"), Some("sessions")) => list_sessions(repl, &marie).await,
        (Some("create"), Some("session")) => create_session(repl, &marie, tokens).await,
        (Some("delete"), Some("session")) => delete_session(repl, &marie, tokens).await,
        (Some("list"), Some("models")) => list_models(repl, &marie).await,
        (Some("create"), Some("model")) => create_model(repl, &marie, tokens).await,
        (Some("delete"), Some("model")) => delete_model(repl, &marie, tokens).await,
        (Some("list"), Some("experts")) => list_experts(repl, &marie).await,
        (Some("create"), Some("expert")) => create_expert(repl, &marie, tokens).await,
        (Some("delete"), Some("expert")) => delete_expert(repl, &marie, tokens).await,
        (Some("set"), Some("expert")) => set_expert(repl, &marie, tokens).await,
        (Some("list"), Some("tools")) => list_tools(repl, &marie).await,
        (Some("list"), Some("graphs")) => list_graphs(repl, &marie).await,
        (Some("create"), Some("graph")) => create_graph(repl, &marie, tokens),
        (Some("disconnect"), _) => {
            repl.pop();
            Ok(())
        }
        _ => Err(CliError::UnknownCommand { state: repl.top().label(), command: tokens.join(" ") }),
    }
}

async fn list_sessions(repl: &mut Repl, marie: &LocalMarie) -> Result<(), CliError> {
    let sessions = marie.sessions.list_sessions().await.map_err(CliError::from_err)?;
    repl.input.print_line(render::format_sessions(&sessions));
    Ok(())
}

async fn create_session(repl: &mut Repl, marie: &LocalMarie, tokens: &[String]) -> Result<(), CliError> {
    match (tokens.get(2).map(String::as_str), tokens.get(3).map(String::as_str)) {
        (Some("shell"), _) => {
            let args = parse_command_args(&tokens[3..], 1)?;
            let expert_id = ExpertId::new(args.positional(0)?);

            let id = marie
                .sessions
                .create_session(NewSessionArgs::Shell(ShellMode::Expert(expert_id)))
                .await
                .map_err(CliError::from_err)?;

            repl.input.print_line(format!("session créée: {id}"));
            repl.push(ReplState::ExecutingSession { marie: marie.clone(), kind: SessionKind::Live(id) });
            Ok(())
        }
        (Some("execute"), Some("graph")) => {
            let args = parse_command_args(&tokens[4..], 1)?;
            let graph_id = GraphId::from(args.positional(0)?.to_string());

            let mut initial: HashMap<ChannelName, serde_json::Value> = HashMap::new();
            for (key, raw) in args.flags() {
                initial.insert(ChannelName::from(key.as_str()), parse_value(raw)?);
            }

            let id = marie
                .sessions
                .create_session(NewSessionArgs::Graph { graph_id, initial })
                .await
                .map_err(CliError::from_err)?;

            repl.input.print_line(format!("session créée: {id}"));
            repl.push(ReplState::ExecutingSession { marie: marie.clone(), kind: SessionKind::Live(id) });
            Ok(())
        }
        (Some("consult"), Some("expert")) => {
            let args = parse_command_args(&tokens[4..], 2)?;
            let expert_id = ExpertId::new(args.positional(0)?);
            let task = args.positional(1)?.to_string();

            repl.push(ReplState::ExecutingSession { marie: marie.clone(), kind: SessionKind::Consult { expert_id, task } });
            Ok(())
        }
        _ => Err(CliError::usage(
            "create session shell <expert-id> | create session execute graph <graph-id> [channel=value ...] | create session consult expert <expert-id> \"<task>\"",
        )),
    }
}

async fn delete_session(repl: &mut Repl, marie: &LocalMarie, tokens: &[String]) -> Result<(), CliError> {
    let args = parse_command_args(&tokens[2..], 1)?;
    let id: SessionId = args.positional(0)?.parse().map_err(CliError::from_err)?;

    marie.sessions.delete_session(id).await.map_err(CliError::from_err)?;
    repl.input.print_line("session supprimée");
    Ok(())
}

async fn list_models(repl: &mut Repl, marie: &LocalMarie) -> Result<(), CliError> {
    let models = marie.models.list().await.map_err(CliError::from_err)?;
    repl.input.print_line(render::format_models(&models));
    Ok(())
}

async fn create_model(repl: &mut Repl, marie: &LocalMarie, tokens: &[String]) -> Result<(), CliError> {
    let args = parse_command_args(&tokens[2..], 2)?;
    let id = ModelId::new(args.positional(0)?);

    if args.positional(1)? != "openai-compat" {
        return Err(CliError::usage("seul le type de modèle 'openai-compat' est supporté"));
    }

    let base_url = args.flag_text("base-url").ok_or_else(|| CliError::usage("base-url manquant"))?;
    let client_id = args.flag_text("client-id").ok_or_else(|| CliError::usage("client-id manquant"))?;
    let api_key = args.flag_text("api-key").ok_or_else(|| CliError::usage("api-key manquant"))?;
    let model = args.flag_text("model").ok_or_else(|| CliError::usage("model manquant"))?;
    let system_prompt = args.flag_text("system-prompt");

    marie
        .models
        .insert(CreateModel::OpenAICompatible { id, base_url, client_id, api_key, model, system_prompt })
        .await
        .map_err(CliError::from_err)?;

    repl.input.print_line("modèle créé");
    Ok(())
}

async fn delete_model(repl: &mut Repl, marie: &LocalMarie, tokens: &[String]) -> Result<(), CliError> {
    let args = parse_command_args(&tokens[2..], 1)?;
    let id = ModelId::new(args.positional(0)?);

    marie.models.delete(&id).await.map_err(CliError::from_err)?;
    repl.input.print_line("modèle supprimé");
    Ok(())
}

async fn list_experts(repl: &mut Repl, marie: &LocalMarie) -> Result<(), CliError> {
    let experts = marie.experts.list().await.map_err(CliError::from_err)?;
    repl.input.print_line(render::format_experts(&experts));
    Ok(())
}

async fn create_expert(repl: &mut Repl, marie: &LocalMarie, tokens: &[String]) -> Result<(), CliError> {
    let args = parse_command_args(&tokens[2..], 1)?;
    let id = ExpertId::new(args.positional(0)?);

    let model_id = ModelId::new(args.flag_text("model").ok_or_else(|| CliError::usage("model manquant"))?);
    let allowed_tools = args.flag_csv("allowed-tools").into_iter().map(ToolId::from).collect();
    let prompt = args.flag_text("prompt").ok_or_else(|| CliError::usage("prompt manquant"))?;

    marie.experts.create(Expert { id, prompt, model_id, allowed_tools }).await.map_err(CliError::from_err)?;
    repl.input.print_line("expert créé");
    Ok(())
}

async fn delete_expert(repl: &mut Repl, marie: &LocalMarie, tokens: &[String]) -> Result<(), CliError> {
    let args = parse_command_args(&tokens[2..], 1)?;
    let id = ExpertId::new(args.positional(0)?);

    marie.experts.delete(&id).await.map_err(CliError::from_err)?;
    repl.input.print_line("expert supprimé");
    Ok(())
}

async fn set_expert(repl: &mut Repl, marie: &LocalMarie, tokens: &[String]) -> Result<(), CliError> {
    let args = parse_command_args(&tokens[2..], 1)?;
    let id = ExpertId::new(args.positional(0)?);

    let mut expert = marie
        .experts
        .get(&id)
        .await
        .map_err(CliError::from_err)?
        .ok_or_else(|| CliError::usage(format!("expert introuvable: {id}")))?;

    if let Some(prompt) = args.flag_text("prompt") {
        expert.prompt = prompt;
    }
    if let Some(model) = args.flag_text("model") {
        expert.model_id = ModelId::new(model);
    }
    if args.flag_str("allowed-tools").is_some() {
        expert.allowed_tools = args.flag_csv("allowed-tools").into_iter().map(ToolId::from).collect();
    }

    marie.experts.replace(expert).await.map_err(CliError::from_err)?;
    repl.input.print_line("expert mis à jour");
    Ok(())
}

async fn list_tools(repl: &mut Repl, marie: &LocalMarie) -> Result<(), CliError> {
    let tools = marie.tools.list().await.map_err(CliError::from_err)?;
    repl.input.print_line(render::format_tools(&tools));
    Ok(())
}

async fn list_graphs(repl: &mut Repl, marie: &LocalMarie) -> Result<(), CliError> {
    let graphs = marie.graphs.list().await.map_err(CliError::from_err)?;
    repl.input.print_line(render::format_graphs(&graphs));
    Ok(())
}

fn create_graph(repl: &mut Repl, marie: &LocalMarie, tokens: &[String]) -> Result<(), CliError> {
    let args = parse_command_args(&tokens[2..], 1)?;
    let id = GraphId::from(args.positional(0)?.to_string());

    let spec = GraphSpec::new(id, NodeId::from("start"), CommonSpec::default());
    repl.push(ReplState::EditingGraph { marie: marie.clone(), spec });
    Ok(())
}
