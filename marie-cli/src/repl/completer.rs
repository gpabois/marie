use std::sync::{Arc, Mutex};

use rustyline::Context;
use rustyline::Helper;
use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;

use crate::repl::state::CommandSpec;

/// Complète le nom d'une commande — les noms du tableau partagé sont
/// multi-mots ("create session shell"), donc la comparaison se fait contre
/// tout le préfixe déjà tapé (`line[..pos]`), pas seulement le dernier mot :
/// un simple "dernier mot" ne suffirait pas à distinguer "create sess<TAB>"
/// de "create graph" au même niveau. Le tableau de commandes visé change à
/// chaque changement d'état du REPL (voir `Repl::push`/`Repl::pop`) — porté
/// par la cellule partagée plutôt que fixé à la construction, pour ne pas
/// avoir à reconstruire l'`Editor`/le fil d'entrée à chaque transition.
pub struct ReplHelper {
    commands: Arc<Mutex<&'static [CommandSpec]>>,
}

impl ReplHelper {
    pub fn new(commands: Arc<Mutex<&'static [CommandSpec]>>) -> Self {
        Self { commands }
    }
}

impl Completer for ReplHelper {
    type Candidate = Pair;

    fn complete(&self, line: &str, pos: usize, _ctx: &Context<'_>) -> rustyline::Result<(usize, Vec<Pair>)> {
        let input = &line[..pos];
        let table = self.commands.lock().expect("le mutex du tableau de commandes n'est jamais empoisonné");

        let matches: Vec<Pair> = table
            .iter()
            .filter(|cmd| cmd.name.len() > input.len() && cmd.name.starts_with(input))
            .map(|cmd| Pair { display: cmd.name.to_string(), replacement: cmd.name[input.len()..].to_string() })
            .collect();

        Ok((pos, matches))
    }
}

impl Hinter for ReplHelper {
    type Hint = String;
}

impl Highlighter for ReplHelper {}

impl Validator for ReplHelper {}

impl Helper for ReplHelper {}
