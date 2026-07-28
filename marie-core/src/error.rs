//! Erreur générique inspirée d'[`anyhow::Error`], mais sérialisable.
//!
//! `anyhow::Error` capture une trait-objet (`Box<dyn StdError + Send +
//! Sync>`) : pratique en local, mais ça ne survit pas à un aller-retour
//! réseau (RPC, diff CRDT, etc.) puisque le type concret de l'erreur
//! d'origine est perdu à la compilation. [`Error`] fait le choix inverse :
//! ne garder que la chaîne des messages (`Display` de l'erreur et de ses
//! `source()` successives), ce qui se sérialise trivialement en un
//! `Vec<String>` et reste lisible une fois désérialisé de l'autre côté,
//! au prix de ne plus pouvoir `downcast` vers le type d'origine.
//!
//! Comme `anyhow::Error`, elle se construit par conversion automatique
//! (`?`) depuis n'importe quel type implémentant `std::error::Error` — voir
//! l'impl générique de [`From`] ci-dessous — et s'enrichit de contexte via
//! [`Context::context`]. Et comme `anyhow::Error`, [`Error`] n'implémente
//! volontairement pas `std::error::Error` : sinon l'impl générique de `From`
//! entrerait en conflit avec l'impl réflexive `impl<T> From<T> for T` du
//! std (`E = Error` satisferait alors les deux à la fois).

use std::fmt;

use serde::{Deserialize, Serialize};

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Chaîne de messages capturée depuis une erreur (et ses causes), portable
/// sur le réseau. Voir la documentation de module pour le choix de
/// conception.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Error {
    /// `chain[0]` est le message de plus haut niveau (le plus de contexte),
    /// chaque élément suivant est la cause du précédent.
    chain: Vec<String>,
}

impl Error {
    /// Construit une erreur ad hoc à partir d'un simple message, sans
    /// erreur source (équivalent à `anyhow!("...")`).
    pub fn msg(message: impl fmt::Display) -> Self {
        Self { chain: vec![message.to_string()] }
    }

    /// Empile un message de contexte au-dessus de la chaîne existante
    /// (équivalent à `anyhow::Context::context`, mais directement sur
    /// `Error` plutôt que sur un `Result`  — voir [`Context`] pour la
    /// version qui s'applique à un `Result<T, E>`).
    pub fn context(mut self, context: impl fmt::Display) -> Self {
        self.chain.insert(0, context.to_string());
        self
    }

    /// Le message de plus haut niveau, sans les causes.
    pub fn message(&self) -> &str {
        &self.chain[0]
    }

    /// La chaîne complète, du message de plus haut niveau vers la cause
    /// racine.
    pub fn chain(&self) -> impl Iterator<Item = &str> {
        self.chain.iter().map(String::as_str)
    }

    /// La cause la plus profonde de la chaîne.
    pub fn root_cause(&self) -> &str {
        self.chain.last().expect("la chaîne d'une `Error` ne peut pas être vide")
    }

    /// Interop avec le reste du code base, encore largement `anyhow`-based.
    /// Ne peut pas être un `impl From<anyhow::Error>` : `anyhow::Error` est
    /// un type étranger, et le coherence checker refuse qu'un type étranger
    /// soit ciblé à la fois par un impl concret et par l'impl générique de
    /// `From` ci-dessous (une prochaine version d'`anyhow` pourrait, en
    /// théorie, lui faire implémenter `std::error::Error`).
    pub fn from_anyhow(err: anyhow::Error) -> Self {
        Self { chain: err.chain().map(ToString::to_string).collect() }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.chain[0])?;
        if self.chain.len() > 1 {
            f.write_str("\n\nCaused by:")?;
            for (i, cause) in self.chain[1..].iter().enumerate() {
                write!(f, "\n    {i}: {cause}")?;
            }
        }
        Ok(())
    }
}

// Volontairement pas d'`impl std::error::Error for Error` — voir la
// documentation de module.

/// Conversion automatique depuis n'importe quelle erreur standard — c'est
/// elle qui permet à `?` de fonctionner comme avec `anyhow::Error`.
impl<E> From<E> for Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(err: E) -> Self {
        let mut chain = vec![err.to_string()];
        let mut source = std::error::Error::source(&err);
        while let Some(cause) = source {
            chain.push(cause.to_string());
            source = cause.source();
        }
        Self { chain }
    }
}

/// Équivalent de `anyhow::Context` : ajoute du contexte à l'erreur portée
/// par un `Result`, sans avoir à d'abord la convertir explicitement en
/// [`Error`].
pub trait Context<T> {
    fn context(self, context: impl fmt::Display) -> Result<T>;
    fn with_context<C, F>(self, context: F) -> Result<T>
    where
        C: fmt::Display,
        F: FnOnce() -> C;
}

impl<T, E> Context<T> for std::result::Result<T, E>
where
    E: Into<Error>,
{
    fn context(self, context: impl fmt::Display) -> Result<T> {
        self.map_err(|err| err.into().context(context))
    }

    fn with_context<C, F>(self, context: F) -> Result<T>
    where
        C: fmt::Display,
        F: FnOnce() -> C,
    {
        self.map_err(|err| err.into().context(context()))
    }
}

/// Équivalent de `anyhow::anyhow!` — mêmes arguments (style `format!`),
/// mais construit une [`Error`] locale plutôt qu'une `anyhow::Error`.
#[macro_export]
macro_rules! err {
    ($($arg:tt)*) => {
        $crate::error::Error::msg(format!($($arg)*))
    };
}

/// Équivalent de `anyhow::bail!` — voir [`err!`].
#[macro_export]
macro_rules! bail {
    ($($arg:tt)*) => {
        return Err($crate::err!($($arg)*).into())
    };
}

/// Équivalent de `anyhow::ensure!` — voir [`err!`].
#[macro_export]
macro_rules! ensure {
    ($cond:expr $(,)?) => {
        $crate::ensure!($cond, concat!("Condition non satisfaite : ", stringify!($cond)))
    };
    ($cond:expr, $($arg:tt)*) => {
        if !($cond) {
            $crate::bail!($($arg)*);
        }
    };
}
