use marie::graph::{NodeId, NodeKindId, NodeSpec};
use marie::session::spec::CommonSpec;

use crate::repl::{Repl, args::parse_command_args, error::CliError, render, state::ReplState};

pub async fn dispatch(repl: &mut Repl, tokens: &[String]) -> Result<(), CliError> {
    match (tokens.first().map(String::as_str), tokens.get(1).map(String::as_str)) {
        (Some("set"), Some("entry")) => set_entry(repl, tokens),
        (Some("list"), Some("nodes")) => list_nodes(repl),
        (Some("create"), Some("node")) => create_node(repl, tokens),
        (Some("connect"), _) => connect(repl, tokens),
        (Some("disconnect"), _) => disconnect(repl, tokens),
        (Some("save"), _) => save(repl).await,
        _ => Err(CliError::UnknownCommand { state: repl.top().label(), command: tokens.join(" ") }),
    }
}

fn set_entry(repl: &mut Repl, tokens: &[String]) -> Result<(), CliError> {
    let args = parse_command_args(&tokens[2..], 1)?;
    let node_id = NodeId::from(args.positional(0)?);

    let Some(ReplState::EditingGraph { spec, .. }) = repl.stack.last_mut() else { unreachable!() };
    spec.entry = node_id;
    Ok(())
}

fn list_nodes(repl: &mut Repl) -> Result<(), CliError> {
    let ReplState::EditingGraph { spec, .. } = repl.top() else { unreachable!() };
    repl.input.print_line(render::format_nodes(spec));
    Ok(())
}

fn create_node(repl: &mut Repl, tokens: &[String]) -> Result<(), CliError> {
    let args = parse_command_args(&tokens[2..], 2)?;
    let node_id = NodeId::from(args.positional(0)?);
    let kind = NodeKindId::from(args.positional(1)?);

    let spec = NodeSpec { kind, common: CommonSpec::default() };
    repl.push(ReplState::EditingNode { node_id, spec });
    Ok(())
}

fn connect(repl: &mut Repl, tokens: &[String]) -> Result<(), CliError> {
    let args = parse_command_args(&tokens[1..], 2)?;
    let a = NodeId::from(args.positional(0)?);
    let b = NodeId::from(args.positional(1)?);

    let Some(ReplState::EditingGraph { spec, .. }) = repl.stack.last_mut() else { unreachable!() };
    spec.add_edge(a, b);
    Ok(())
}

fn disconnect(repl: &mut Repl, tokens: &[String]) -> Result<(), CliError> {
    let args = parse_command_args(&tokens[1..], 2)?;
    let a = NodeId::from(args.positional(0)?);

    let Some(ReplState::EditingGraph { spec, .. }) = repl.stack.last_mut() else { unreachable!() };
    spec.edges.remove(&a);
    Ok(())
}

/// Dépile l'état — soit une nouvelle publication (`Graphs::insert`) si
/// `spec.id` n'a encore aucune version active, soit une nouvelle version
/// (`Graphs::replace`) sinon (voir `Graphs::latest`) : "sauvegarde ou créé"
/// de la spécification.
async fn save(repl: &mut Repl) -> Result<(), CliError> {
    let Some(ReplState::EditingGraph { marie, spec }) = repl.pop() else { unreachable!() };
    let id = spec.id.clone();

    let exists = marie.graphs.latest(&id).await.map_err(CliError::from_err)?.is_some();
    if exists {
        marie.graphs.replace(spec).await.map_err(CliError::from_err)?;
    } else {
        marie.graphs.insert(spec).await.map_err(CliError::from_err)?;
    }

    repl.input.print_line("graphe sauvegardé");
    Ok(())
}
