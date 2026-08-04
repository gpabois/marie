use std::sync::{Arc, Mutex};

use rustyline::{Editor, ExternalPrinter, history::DefaultHistory};
use tokio::sync::{mpsc, oneshot};

use crate::repl::completer::ReplHelper;
use crate::repl::state::CommandSpec;

enum InputCmd {
    ReadLine { prompt: String, reply: oneshot::Sender<rustyline::Result<String>> },
    Print(String),
}

/// Poignée async vers l'`Editor` rustyline, qui vit sur un `std::thread`
/// dédié pour toute la durée du process (pas un `spawn_blocking` ponctuel :
/// c'est une ressource qui possède l'état du terminal, pas une tâche
/// bloquante ordinaire) — rustyline 14 n'a pas de `readline` async ;
/// `readline`/`print` restent donc les deux seuls points où le code
/// traverse la frontière sync/async, via ce canal. `create_external_printer`
/// laisse imprimer par-dessus une invite en cours sans la corrompre — géré
/// ici plutôt qu'exposé au clonage/`Send` incertain de son type de retour,
/// pour ne dépendre que de primitives déjà connues (canaux `tokio`).
#[derive(Clone)]
pub struct InputHandle {
    cmd_tx: mpsc::UnboundedSender<InputCmd>,
}

impl InputHandle {
    pub fn spawn(commands: Arc<Mutex<&'static [CommandSpec]>>) -> Self {
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<InputCmd>();

        std::thread::spawn(move || {
            let mut rl = match Editor::<ReplHelper, DefaultHistory>::new() {
                Ok(rl) => rl,
                Err(err) => {
                    eprintln!("impossible d'initialiser rustyline: {err}");
                    return;
                }
            };
            rl.set_helper(Some(ReplHelper::new(commands)));

            let mut printer = match rl.create_external_printer() {
                Ok(printer) => Some(printer),
                Err(err) => {
                    eprintln!("impossible de créer l'external printer rustyline (les logs seront imprimés sans egard pour la ligne en cours): {err}");
                    None
                }
            };

            while let Some(cmd) = cmd_rx.blocking_recv() {
                match cmd {
                    InputCmd::ReadLine { prompt, reply } => {
                        let line = rl.readline(&prompt);
                        if let Ok(text) = &line {
                            let _ = rl.add_history_entry(text.as_str());
                        }
                        let _ = reply.send(line);
                    }
                    InputCmd::Print(text) => match printer.as_mut() {
                        Some(printer) => {
                            let _ = printer.print(text);
                        }
                        None => println!("{text}"),
                    },
                }
            }
        });

        Self { cmd_tx }
    }

    pub async fn read_line(&self, prompt: &str) -> rustyline::Result<String> {
        let (tx, rx) = oneshot::channel();
        if self.cmd_tx.send(InputCmd::ReadLine { prompt: prompt.to_string(), reply: tx }).is_err() {
            return Err(rustyline::error::ReadlineError::Eof);
        }
        rx.await.unwrap_or(Err(rustyline::error::ReadlineError::Eof))
    }

    pub fn print_line(&self, s: impl std::fmt::Display) {
        let _ = self.cmd_tx.send(InputCmd::Print(s.to_string()));
    }
}
