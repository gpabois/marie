pub mod args;
pub mod completer;
pub mod error;
pub mod input;
pub mod render;
pub mod state;
pub mod tokenizer;

use std::sync::{Arc, Mutex};

use rustyline::error::ReadlineError;

use crate::states::{editing_graph, editing_node, executing_session, local_connected, root};
use error::CliError;
use input::InputHandle;
use state::{CommandSpec, ReplState};
use tokenizer::tokenize;

/// REPL à pile d'états (voir [`ReplState`]) — chaque commande est routée
/// vers le seul module `states::*` compétent pour l'état actuellement en
/// haut de pile (voir [`Self::dispatch`]), pas vers un espace de noms de
/// commandes partagé : une commande absente du tableau de l'état courant
/// est simplement inconnue, pas explicitement rejetée par état.
pub struct Repl {
    pub(crate) stack: Vec<ReplState>,
    pub input: InputHandle,
    completer_table: Arc<Mutex<&'static [CommandSpec]>>,
    pub verbose: bool,
}

impl Repl {
    pub fn new(verbose: bool) -> Self {
        let completer_table = Arc::new(Mutex::new(ReplState::Root.commands()));
        let input = InputHandle::spawn(completer_table.clone());
        Self { stack: vec![ReplState::Root], input, completer_table, verbose }
    }

    fn sync_completer_table(&self) {
        let top = self.stack.last().expect("la pile n'est jamais vide");
        *self.completer_table.lock().expect("le mutex du tableau de commandes n'est jamais empoisonné") = top.commands();
    }

    /// Empile un nouvel état et met à jour la table de commandes partagée
    /// avec le compléteur (voir `repl::completer::ReplHelper`) — préférer
    /// ceci à une manipulation directe de `self.stack` dès qu'un état est
    /// réellement empilé/dépilé (par opposition à une mutation en place de
    /// l'état courant, ex. `spec.edges.insert(...)` dans `EditingGraph`, qui
    /// ne change pas le jeu de commandes disponibles).
    pub fn push(&mut self, state: ReplState) {
        self.stack.push(state);
        self.sync_completer_table();
    }

    /// Dépile l'état courant, sauf si c'est `Root` (jamais dépilé — c'est la
    /// base de la pile). Renvoie `None` sans rien changer si on est déjà à
    /// `Root`.
    pub fn pop(&mut self) -> Option<ReplState> {
        if self.stack.len() <= 1 {
            return None;
        }
        let popped = self.stack.pop();
        self.sync_completer_table();
        popped
    }

    pub fn top(&self) -> &ReplState {
        self.stack.last().expect("la pile n'est jamais vide")
    }

    fn format_prompt(&self) -> String {
        let parts: Vec<String> = self.stack.iter().filter(|s| !matches!(s, ReplState::Root)).map(|s| s.label()).collect();

        if parts.is_empty() { "marie> ".to_string() } else { format!("marie({})> ", parts.join("/")) }
    }

    pub async fn run(&mut self) {
        loop {
            if matches!(self.top(), ReplState::ExecutingSession { .. }) {
                let Some(ReplState::ExecutingSession { marie, kind }) = self.stack.pop() else { unreachable!() };
                self.sync_completer_table();
                executing_session::run(self, marie, kind).await;
                continue;
            }

            let prompt = self.format_prompt();
            let line = match self.input.read_line(&prompt).await {
                Ok(line) => line,
                Err(ReadlineError::Interrupted) => {
                    self.input.print_line("^C");
                    continue;
                }
                Err(ReadlineError::Eof) => {
                    if self.handle_eof() {
                        break;
                    }
                    continue;
                }
                Err(err) => {
                    self.input.print_line(format!("erreur d'entrée: {err}"));
                    break;
                }
            };

            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let tokens = match tokenize(line) {
                Ok(tokens) => tokens,
                Err(err) => {
                    self.print_error(&err);
                    continue;
                }
            };

            if tokens.first().map(String::as_str) == Some("help") {
                self.print_help();
                continue;
            }
            if tokens.first().map(String::as_str) == Some("exit") && matches!(self.top(), ReplState::Root) {
                break;
            }

            if let Err(err) = self.dispatch(&tokens).await {
                self.print_error(&err);
            }
        }
    }

    /// `true` si le REPL doit se terminer (Ctrl-D à `Root`). Dans tous les
    /// autres états, Ctrl-D est la "sortie naturelle" de cet état — voir le
    /// détail par variante.
    fn handle_eof(&mut self) -> bool {
        match self.top() {
            ReplState::Root => true,
            ReplState::LocalConnected { .. } => {
                self.pop();
                false
            }
            ReplState::EditingGraph { .. } | ReplState::EditingNode { .. } => {
                self.input.print_line("abandon de l'édition (non sauvegardé)");
                self.pop();
                false
            }
            // Géré directement dans `Self::run` avant d'atteindre `readline`
            // (voir la branche `ExecutingSession` en tête de boucle) : ce
            // bras ne devrait jamais être atteint.
            ReplState::ExecutingSession { .. } => false,
        }
    }

    async fn dispatch(&mut self, tokens: &[String]) -> Result<(), CliError> {
        match self.top() {
            ReplState::Root => root::dispatch(self, tokens).await,
            ReplState::LocalConnected { .. } => local_connected::dispatch(self, tokens).await,
            ReplState::EditingGraph { .. } => editing_graph::dispatch(self, tokens).await,
            ReplState::EditingNode { .. } => editing_node::dispatch(self, tokens).await,
            ReplState::ExecutingSession { .. } => unreachable!("géré directement dans Repl::run"),
        }
    }

    fn print_help(&self) {
        for cmd in self.top().commands() {
            self.input.print_line(format!("{:<45} {}", cmd.name, cmd.usage));
        }
    }

    fn print_error(&self, err: &CliError) {
        self.input.print_line(format!("erreur: {err}"));
        if self.verbose {
            self.input.print_line(format!("{err:?}"));
        }
    }
}
