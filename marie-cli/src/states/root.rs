use marie::{LocalMarie, LocalMarieArgs, entrypoint::DatabaseMode};

use crate::repl::{Repl, args::parse_command_args, error::CliError, state::ReplState};

pub async fn dispatch(repl: &mut Repl, tokens: &[String]) -> Result<(), CliError> {
    match tokens.first().map(String::as_str) {
        Some("connect") => match tokens.get(1).map(String::as_str) {
            Some("local") => connect_local(repl, tokens).await,
            Some("remote") => connect_remote(repl, tokens),
            _ => Err(CliError::usage("connect local <in-memory|postgres> | connect remote <addr>")),
        },
        _ => Err(CliError::UnknownCommand { state: repl.top().label(), command: tokens.join(" ") }),
    }
}

async fn connect_local(repl: &mut Repl, tokens: &[String]) -> Result<(), CliError> {
    let args = parse_command_args(&tokens[2..], 1)?;
    let mode = args.positional(0)?;

    let database_mode = match mode {
        "in-memory" => DatabaseMode::InMemory,
        "postgres" => {
            let url = std::env::var("DATABASE_URL")
                .map_err(|_| CliError::usage("DATABASE_URL doit être défini dans l'environnement (ou le fichier .env) pour connect local postgres"))?;
            DatabaseMode::Postgres(url)
        }
        other => return Err(CliError::usage(format!("mode de base de données inconnu: {other} (attendu in-memory|postgres)"))),
    };

    let (epochs, current_epoch) = crate::secret::load_or_generate_epoch();

    let marie_args = LocalMarieArgs::builder().epochs(epochs).current_epoch(current_epoch).database_mode(database_mode).build();

    let marie: LocalMarie = LocalMarie::new(marie_args).await.map_err(CliError::from_err)?;

    repl.input.print_line("connecté (local)");
    repl.push(ReplState::LocalConnected { marie });

    Ok(())
}

fn connect_remote(repl: &mut Repl, tokens: &[String]) -> Result<(), CliError> {
    let args = parse_command_args(&tokens[2..], 1)?;
    let addr = args.positional(0)?;
    repl.input.print_line(format!("connect remote n'est pas encore implémenté (adresse demandée: {addr})"));
    Ok(())
}
