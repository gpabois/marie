use thiserror::Error;

/// Erreur du REPL — englobe les erreurs de grammaire (tokenizer/args), les
/// erreurs de routage (commande inconnue dans l'état courant) et les
/// erreurs remontées par `marie-core` (catalogue, session, hitl, etc.,
/// toutes converties via [`Self::from_err`] plutôt que `From`/`?`
/// automatique : ces types varient d'un appel à l'autre (`ModelError`,
/// `ExpertError`, `CatalogError`, `SessionError`, ...) et n'ont en commun
/// que `Debug`/`Display` via `thiserror`, pas un type d'erreur unique vers
/// lequel converger sans un `impl<E: Trait> From<E> for CliError`
/// générique qui entrerait en conflit avec l'`impl<T> From<T> for T` du
/// coeur de std dès que `E = CliError`).
#[derive(Debug, Error)]
pub enum CliError {
    #[error("{0}")]
    Usage(String),
    #[error("\"{command}\" n'est pas valide dans l'état '{state}' — essayez 'help'")]
    UnknownCommand { state: String, command: String },
    #[error("{0}")]
    Marie(String),
    #[error("{0}")]
    Io(String),
}

impl CliError {
    pub fn usage(msg: impl Into<String>) -> Self {
        CliError::Usage(msg.into())
    }

    pub fn from_err(err: impl std::fmt::Debug) -> Self {
        CliError::Marie(format!("{err:?}"))
    }
}

impl From<rustyline::error::ReadlineError> for CliError {
    fn from(err: rustyline::error::ReadlineError) -> Self {
        CliError::Io(err.to_string())
    }
}
