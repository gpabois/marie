use std::collections::HashMap;

use marie::session::channel::Reducer;

use crate::repl::error::CliError;

/// Arguments d'une commande déjà découpée en tokens (voir
/// `repl::tokenizer::tokenize`) — un préfixe positionnel fixe suivi d'un
/// nombre libre de drapeaux `key=value` non ordonnés, la forme commune à
/// toutes les commandes du REPL (voir la grammaire dans le plan). Les
/// valeurs brutes conservent leurs guillemets/crochets d'origine ; c'est
/// aux accesseurs (`flag_str`, `flag_value`, ...) de les interpréter.
pub struct CommandArgs {
    positionals: Vec<String>,
    flags: HashMap<String, String>,
}

pub fn parse_command_args(tokens: &[String], n_positional: usize) -> Result<CommandArgs, CliError> {
    if tokens.len() < n_positional {
        return Err(CliError::usage(format!(
            "attendu au moins {n_positional} argument(s) positionnel(s), trouvé {}",
            tokens.len()
        )));
    }

    let positionals = tokens[..n_positional].iter().map(|t| unquote(t)).collect();

    let mut flags = HashMap::new();
    for token in &tokens[n_positional..] {
        let Some((key, value)) = token.split_once('=') else {
            return Err(CliError::usage(format!("argument invalide (attendu key=value): {token}")));
        };
        if flags.insert(key.to_string(), value.to_string()).is_some() {
            return Err(CliError::usage(format!("argument dupliqué: {key}")));
        }
    }

    Ok(CommandArgs { positionals, flags })
}

fn unquote(raw: &str) -> String {
    let s = raw.trim();
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        s[1..s.len() - 1].replace("\\\"", "\"")
    } else {
        s.to_string()
    }
}

impl CommandArgs {
    pub fn positional(&self, i: usize) -> Result<&str, CliError> {
        self.positionals
            .get(i)
            .map(|s| s.as_str())
            .ok_or_else(|| CliError::usage(format!("argument positionnel manquant #{i}")))
    }

    pub fn flag_str(&self, key: &str) -> Option<&str> {
        self.flags.get(key).map(|v| v.as_str())
    }

    /// Tous les drapeaux `key=value` tels que tapés (bruts, non
    /// interprétés) — pour les commandes où les clés elles-mêmes sont
    /// arbitraires (ex. `create session execute graph` où chaque drapeau
    /// est un nom de canal du graphe cible, pas connu à l'avance).
    pub fn flags(&self) -> &HashMap<String, String> {
        &self.flags
    }

    /// Comme [`Self::flag_str`], mais retire les guillemets englobants s'il
    /// y en a — pour les valeurs texte libres (`prompt="..."`,
    /// `system-prompt="..."`).
    pub fn flag_text(&self, key: &str) -> Option<String> {
        self.flags.get(key).map(|v| unquote(v))
    }

    pub fn flag_bool(&self, key: &str, default: bool) -> Result<bool, CliError> {
        Ok(self.flag_bool_opt(key)?.unwrap_or(default))
    }

    /// Comme [`Self::flag_bool`], mais distingue "absent" de "présent et
    /// faux" — nécessaire pour `set channel`, où un drapeau non fourni doit
    /// laisser l'appartenance du canal inchangée plutôt que la remettre à
    /// une valeur par défaut.
    pub fn flag_bool_opt(&self, key: &str) -> Result<Option<bool>, CliError> {
        match self.flag_str(key) {
            None => Ok(None),
            Some("true") => Ok(Some(true)),
            Some("false") => Ok(Some(false)),
            Some(other) => Err(CliError::usage(format!("valeur booléenne invalide pour {key}: {other}"))),
        }
    }

    /// Liste séparée par des virgules (`allowed-tools=a,b,c`) — pas la
    /// grammaire crochets (`[...]`) des valeurs de canal, volontairement
    /// plus simple puisque les identifiants d'outils ne contiennent jamais
    /// de virgule.
    pub fn flag_csv(&self, key: &str) -> Vec<String> {
        match self.flag_str(key) {
            None => Vec::new(),
            Some(s) => s.split(',').map(|v| v.trim().to_string()).filter(|v| !v.is_empty()).collect(),
        }
    }

    pub fn flag_reducer(&self, key: &str) -> Result<Option<Reducer>, CliError> {
        match self.flag_str(key) {
            None => Ok(None),
            Some("append") => Ok(Some(Reducer::Append)),
            Some("lww") => Ok(Some(Reducer::LastWriteWins)),
            Some("max") => Ok(Some(Reducer::Max)),
            Some(other) => Err(CliError::usage(format!("reducer inconnu: {other} (attendu append|lww|max)"))),
        }
    }

    /// Grammaire de valeur par défaut d'un canal (`default=...`) — voir
    /// [`parse_value`].
    pub fn flag_value(&self, key: &str) -> Result<Option<serde_json::Value>, CliError> {
        match self.flag_str(key) {
            None => Ok(None),
            Some(s) => Ok(Some(parse_value(s)?)),
        }
    }
}

/// `""` -> chaîne vide ; un entier/décimal -> nombre ; `[v1, v2, ...]`
/// (récursif, virgules de premier niveau uniquement) -> liste ; sinon ->
/// chaîne brute (guillemets retirés s'il y en a).
pub fn parse_value(raw: &str) -> Result<serde_json::Value, CliError> {
    let s = raw.trim();

    if s.is_empty() || s == "\"\"" {
        return Ok(serde_json::Value::String(String::new()));
    }

    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        return Ok(serde_json::Value::String(unquote(s)));
    }

    if s.starts_with('[') && s.ends_with(']') {
        let inner = &s[1..s.len() - 1];
        let mut values = Vec::new();
        for item in split_top_level_commas(inner) {
            let item = item.trim();
            if item.is_empty() {
                continue;
            }
            values.push(parse_value(item)?);
        }
        return Ok(serde_json::Value::Array(values));
    }

    if let Ok(i) = s.parse::<i64>() {
        return Ok(serde_json::Value::from(i));
    }

    if let Ok(f) = s.parse::<f64>() {
        return Ok(serde_json::Value::from(f));
    }

    Ok(serde_json::Value::String(s.to_string()))
}

fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;

    for (i, c) in s.char_indices() {
        match c {
            '[' => depth += 1,
            ']' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_empty_string() {
        assert_eq!(parse_value("\"\"").unwrap(), serde_json::json!(""));
    }

    #[test]
    fn parses_integer() {
        assert_eq!(parse_value("42").unwrap(), serde_json::json!(42));
    }

    #[test]
    fn parses_float() {
        assert_eq!(parse_value("0.99").unwrap(), serde_json::json!(0.99));
    }

    #[test]
    fn parses_list() {
        assert_eq!(parse_value("[1, 2, 3]").unwrap(), serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn parses_empty_list() {
        assert_eq!(parse_value("[]").unwrap(), serde_json::json!([]));
    }

    #[test]
    fn parses_bare_string() {
        assert_eq!(parse_value("hello").unwrap(), serde_json::json!("hello"));
    }

    #[test]
    fn flag_csv_trims_and_drops_empties() {
        let args = parse_command_args(&["e1".to_string(), "allowed-tools=a, b ,c".to_string()], 1).unwrap();
        assert_eq!(args.flag_csv("allowed-tools"), vec!["a", "b", "c"]);
    }
}
