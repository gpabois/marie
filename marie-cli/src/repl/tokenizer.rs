use crate::repl::error::CliError;

/// Découpe une ligne de commande en tokens — espaces comme séparateurs,
/// sauf à l'intérieur d'une chaîne entre guillemets (`"..."`, échappable
/// via `\"`, peut commencer au milieu d'un token comme
/// `system-prompt="a b"`) ou d'une liste entre crochets (`[...]`,
/// profondeur suivie pour gérer une éventuelle imbrication). L'état
/// "guillemet" a priorité sur l'état "crochet" : un `[` rencontré dans une
/// chaîne entre guillemets est un caractère littéral, pas une ouverture de
/// liste.
pub fn tokenize(line: &str) -> Result<Vec<String>, CliError> {
    enum Mode {
        Normal,
        Quoted,
        Bracketed(u32),
    }

    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut has_current = false;
    let mut mode = Mode::Normal;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        match mode {
            Mode::Normal => {
                if c.is_whitespace() {
                    if has_current {
                        tokens.push(std::mem::take(&mut current));
                        has_current = false;
                    }
                } else if c == '"' {
                    current.push(c);
                    has_current = true;
                    mode = Mode::Quoted;
                } else if c == '[' {
                    current.push(c);
                    has_current = true;
                    mode = Mode::Bracketed(1);
                } else {
                    current.push(c);
                    has_current = true;
                }
            }
            Mode::Quoted => {
                if c == '\\' && chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next();
                } else if c == '"' {
                    current.push(c);
                    mode = Mode::Normal;
                } else {
                    current.push(c);
                }
            }
            Mode::Bracketed(depth) => {
                if c == '[' {
                    current.push(c);
                    mode = Mode::Bracketed(depth + 1);
                } else if c == ']' {
                    current.push(c);
                    mode = if depth <= 1 { Mode::Normal } else { Mode::Bracketed(depth - 1) };
                } else {
                    current.push(c);
                }
            }
        }
    }

    match mode {
        Mode::Quoted => return Err(CliError::Usage("guillemet non fermé".to_string())),
        Mode::Bracketed(_) => return Err(CliError::Usage("crochet non fermé".to_string())),
        Mode::Normal => {}
    }

    if has_current {
        tokens.push(current);
    }

    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_on_whitespace() {
        assert_eq!(tokenize("create model foo").unwrap(), vec!["create", "model", "foo"]);
    }

    #[test]
    fn keeps_quoted_spaces_together() {
        assert_eq!(
            tokenize(r#"create expert e1 prompt="be helpful please""#).unwrap(),
            vec!["create", "expert", "e1", r#"prompt="be helpful please""#]
        );
    }

    #[test]
    fn keeps_bracketed_lists_together() {
        assert_eq!(
            tokenize("add channel foo default=[1, 2, 3]").unwrap(),
            vec!["add", "channel", "foo", "default=[1, 2, 3]"]
        );
    }

    #[test]
    fn rejects_unclosed_quote() {
        assert!(tokenize(r#"create expert e1 prompt="oops"#).is_err());
    }
}
