mod repl;
mod secret;
mod states;

use clap::{Parser, Subcommand};

use repl::Repl;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
    /// Affiche la chaîne de causes complète des erreurs (via `Debug`),
    /// pas seulement leur message de premier niveau.
    #[arg(long, global = true)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Command {
    /// Passe en mode REPL interactif.
    Interactive,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let cli = Cli::parse();

    match cli.command {
        Command::Interactive => {
            let mut repl = Repl::new(cli.verbose);
            repl.run().await;
        }
    }
}
