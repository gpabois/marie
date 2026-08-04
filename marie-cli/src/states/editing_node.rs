use marie::session::channel::{ChannelName, ChannelSpec, Reducer};

use crate::repl::{Repl, args::parse_command_args, error::CliError, state::ReplState};

pub async fn dispatch(repl: &mut Repl, tokens: &[String]) -> Result<(), CliError> {
    match (tokens.first().map(String::as_str), tokens.get(1).map(String::as_str)) {
        (Some("add"), Some("channel")) => add_channel(repl, tokens),
        (Some("set"), Some("channel")) => set_channel(repl, tokens),
        (Some("save"), _) => save(repl),
        _ => Err(CliError::UnknownCommand { state: repl.top().label(), command: tokens.join(" ") }),
    }
}

fn add_channel(repl: &mut Repl, tokens: &[String]) -> Result<(), CliError> {
    let args = parse_command_args(&tokens[2..], 1)?;
    let name = ChannelName::from(args.positional(0)?);
    let reducer = args.flag_reducer("reducer")?.unwrap_or(Reducer::LastWriteWins);
    let exported = args.flag_bool("exported", false)?;
    let inherited = args.flag_bool("inherited", false)?;
    let imported = args.flag_bool("imported", false)?;
    let default = args.flag_value("default")?;

    let Some(ReplState::EditingNode { spec, .. }) = repl.stack.last_mut() else { unreachable!() };

    spec.common.channels.push(ChannelSpec::new(name.clone(), reducer));
    if exported {
        spec.common.exported_channels.push(name.clone());
    }
    if inherited {
        spec.common.inherited_channels.push(name.clone());
    }
    if imported {
        spec.common.imported_channels.push(name.clone());
    }
    if let Some(value) = default {
        spec.common.default_values.insert(name, value);
    }

    Ok(())
}

fn set_channel(repl: &mut Repl, tokens: &[String]) -> Result<(), CliError> {
    let args = parse_command_args(&tokens[2..], 1)?;
    let name = ChannelName::from(args.positional(0)?);
    let new_reducer = args.flag_reducer("reducer")?;
    let exported = args.flag_bool_opt("exported")?;
    let inherited = args.flag_bool_opt("inherited")?;
    let imported = args.flag_bool_opt("imported")?;

    let Some(ReplState::EditingNode { spec, .. }) = repl.stack.last_mut() else { unreachable!() };

    let idx = spec
        .common
        .channels
        .iter()
        .position(|c| c.name() == &name)
        .ok_or_else(|| CliError::usage(format!("canal introuvable: {name}")))?;

    let reducer = new_reducer.unwrap_or_else(|| spec.common.channels[idx].reducer().clone());
    spec.common.channels[idx] = ChannelSpec::new(name.clone(), reducer);

    if let Some(flag) = exported {
        set_membership(&mut spec.common.exported_channels, &name, flag);
    }
    if let Some(flag) = inherited {
        set_membership(&mut spec.common.inherited_channels, &name, flag);
    }
    if let Some(flag) = imported {
        set_membership(&mut spec.common.imported_channels, &name, flag);
    }

    Ok(())
}

fn set_membership(list: &mut Vec<ChannelName>, name: &ChannelName, present: bool) {
    let already = list.iter().any(|n| n == name);
    if present && !already {
        list.push(name.clone());
    } else if !present && already {
        list.retain(|n| n != name);
    }
}

/// Dépile l'état et commit le noeud en cours d'édition dans le graphe
/// désormais de nouveau en haut de pile.
fn save(repl: &mut Repl) -> Result<(), CliError> {
    let Some(ReplState::EditingNode { node_id, spec }) = repl.pop() else { unreachable!() };

    let Some(ReplState::EditingGraph { spec: graph, .. }) = repl.stack.last_mut() else {
        unreachable!("EditingNode est toujours empilé directement sur EditingGraph")
    };
    graph.nodes.insert(node_id, spec);

    repl.input.print_line("noeud enregistré");
    Ok(())
}
