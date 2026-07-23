use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Type de réponse attendu pour une [`Question`] — détermine à la fois le
/// composant présenté à l'humain (côté passerelle) et la forme attendue de
/// l'[`Answer`] correspondante (voir [`validate_answers`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QuestionKind {
    /// Réponse libre, sur une ligne (ex: un nom, un identifiant).
    ShortText,
    /// Réponse libre, multi-lignes (ex: une explication, un correctif).
    LongText,
    /// Un choix unique parmi `options`, présenté comme une liste déroulante.
    Select { options: Vec<String> },
    /// Un choix unique parmi `options`, présenté comme des boutons radio.
    Radio { options: Vec<String> },
    /// Un ou plusieurs choix parmi `options`, présentés comme des cases à
    /// cocher — aucun choix coché est une réponse valide (voir
    /// [`Answer::Multiple`]).
    Checkboxes { options: Vec<String> },
    /// Un fichier à téléverser, optionnellement restreint à certaines
    /// extensions (`accept`, ex: `[".pdf", ".png"]` — vide : tout fichier
    /// accepté). La réponse (voir [`Answer::Single`]) ne porte que le nom du
    /// fichier : son contenu transite hors de ce message, jamais dans la
    /// réponse elle-même.
    FileUpload { accept: Vec<String> },
}

/// Une question d'un formulaire [`crate::state_graph::hitl::HitlFrame`].
/// `key` identifie la question au sein du formulaire (voir
/// [`validate_answers`]) — choisie par l'appelant plutôt que générée, pour
/// rester lisible côté passerelle humaine sans avoir à conserver une
/// correspondance question ↔ id (ex: `"root_cause"` plutôt qu'un
/// [`crate::id::ID`] opaque).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Question {
    pub key: String,
    pub label: String,
    #[serde(flatten)]
    pub kind: QuestionKind,
}

impl Question {
    #[must_use]
    pub fn short_text(key: impl Into<String>, label: impl Into<String>) -> Self {
        Self { key: key.into(), label: label.into(), kind: QuestionKind::ShortText }
    }

    #[must_use]
    pub fn long_text(key: impl Into<String>, label: impl Into<String>) -> Self {
        Self { key: key.into(), label: label.into(), kind: QuestionKind::LongText }
    }

    #[must_use]
    pub fn select(key: impl Into<String>, label: impl Into<String>, options: Vec<String>) -> Self {
        Self { key: key.into(), label: label.into(), kind: QuestionKind::Select { options } }
    }

    #[must_use]
    pub fn radio(key: impl Into<String>, label: impl Into<String>, options: Vec<String>) -> Self {
        Self { key: key.into(), label: label.into(), kind: QuestionKind::Radio { options } }
    }

    #[must_use]
    pub fn checkboxes(key: impl Into<String>, label: impl Into<String>, options: Vec<String>) -> Self {
        Self { key: key.into(), label: label.into(), kind: QuestionKind::Checkboxes { options } }
    }

    /// `accept` : extensions de fichier acceptées (ex: `vec![".pdf".into()]`) —
    /// vide : tout fichier accepté (voir [`QuestionKind::FileUpload`]).
    #[must_use]
    pub fn file_upload(key: impl Into<String>, label: impl Into<String>, accept: Vec<String>) -> Self {
        Self { key: key.into(), label: label.into(), kind: QuestionKind::FileUpload { accept } }
    }
}

/// Réponse à une [`Question`] — `Single` pour `ShortText`/`LongText`/
/// `Select`/`Radio` (une valeur), `Multiple` pour `Checkboxes` (l'ensemble
/// des choix cochés, éventuellement vide). Voir [`validate_answers`] pour la
/// correspondance imposée entre [`QuestionKind`] et cette forme.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Answer {
    Single(String),
    Multiple(Vec<String>),
}

/// Vérifie que `answers` répond à chacune de `questions` avec une valeur du
/// type attendu (voir [`QuestionKind`]) — helper côté appelant/passerelle
/// (ex. avant de soumettre à [`crate::session::client::SessionClient::report_user_input`]) :
/// [`crate::session::server`] ne valide volontairement pas lui-même (voir la
/// doc de `report_user_input`, c'est ce qui permet à un input spontané de
/// partager la même mutation qu'une réponse structurée sans avoir à
/// satisfaire un schéma qu'il ne connaît pas).
pub fn validate_answers(questions: &[Question], answers: &HashMap<String, Answer>) -> Result<(), String> {
    for question in questions {
        let Some(answer) = answers.get(&question.key) else {
            return Err(format!("réponse manquante pour '{}'", question.key));
        };

        match (&question.kind, answer) {
            (QuestionKind::ShortText | QuestionKind::LongText, Answer::Single(_)) => {}
            (QuestionKind::Select { options } | QuestionKind::Radio { options }, Answer::Single(choice)) => {
                if !options.contains(choice) {
                    return Err(format!("'{choice}' n'est pas une option valide pour '{}'", question.key));
                }
            }
            (QuestionKind::Checkboxes { options }, Answer::Multiple(choices)) => {
                if let Some(invalid) = choices.iter().find(|choice| !options.contains(choice)) {
                    return Err(format!("'{invalid}' n'est pas une option valide pour '{}'", question.key));
                }
            }
            (QuestionKind::FileUpload { accept }, Answer::Single(filename)) => {
                if filename.trim().is_empty() {
                    return Err(format!("nom de fichier manquant pour '{}'", question.key));
                }
                let accepted = accept.is_empty() || accept.iter().any(|ext| filename.to_lowercase().ends_with(&ext.to_lowercase()));
                if !accepted {
                    return Err(format!("'{filename}' n'a pas une extension acceptée pour '{}'", question.key));
                }
            }
            _ => {
                return Err(format!("type de réponse inattendu pour '{}'", question.key));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_questions() -> Vec<Question> {
        vec![
            Question::short_text("name", "Votre nom ?"),
            Question::select("env", "Environnement ?", vec!["prod".into(), "staging".into()]),
            Question::checkboxes("notify", "Notifier qui ?", vec!["slack".into(), "email".into()]),
        ]
    }

    #[test]
    fn test_validate_answers_accepts_matching_answers() {
        let questions = sample_questions();
        let answers = HashMap::from([
            ("name".to_string(), Answer::Single("Ada".to_string())),
            ("env".to_string(), Answer::Single("prod".to_string())),
            ("notify".to_string(), Answer::Multiple(vec!["slack".to_string()])),
        ]);

        assert!(validate_answers(&questions, &answers).is_ok());
    }

    #[test]
    fn test_validate_answers_rejects_missing_answer() {
        let questions = sample_questions();
        let answers = HashMap::from([("name".to_string(), Answer::Single("Ada".to_string()))]);

        assert!(validate_answers(&questions, &answers).is_err());
    }

    #[test]
    fn test_validate_answers_rejects_choice_outside_options() {
        let questions = sample_questions();
        let answers = HashMap::from([
            ("name".to_string(), Answer::Single("Ada".to_string())),
            ("env".to_string(), Answer::Single("dev".to_string())),
            ("notify".to_string(), Answer::Multiple(vec![])),
        ]);

        assert!(validate_answers(&questions, &answers).is_err());
    }

    #[test]
    fn test_validate_answers_rejects_wrong_answer_shape() {
        let questions = sample_questions();
        let answers = HashMap::from([
            ("name".to_string(), Answer::Multiple(vec!["Ada".to_string()])),
            ("env".to_string(), Answer::Single("prod".to_string())),
            ("notify".to_string(), Answer::Multiple(vec![])),
        ]);

        assert!(validate_answers(&questions, &answers).is_err());
    }

    fn file_upload_questions(accept: Vec<String>) -> Vec<Question> {
        vec![Question::file_upload("attachment", "Joindre un fichier", accept)]
    }

    #[test]
    fn test_validate_answers_accepts_matching_file_extension() {
        let questions = file_upload_questions(vec![".pdf".to_string(), ".png".to_string()]);
        let answers = HashMap::from([("attachment".to_string(), Answer::Single("rapport.PDF".to_string()))]);

        assert!(validate_answers(&questions, &answers).is_ok());
    }

    #[test]
    fn test_validate_answers_rejects_unaccepted_file_extension() {
        let questions = file_upload_questions(vec![".pdf".to_string()]);
        let answers = HashMap::from([("attachment".to_string(), Answer::Single("photo.jpg".to_string()))]);

        assert!(validate_answers(&questions, &answers).is_err());
    }

    #[test]
    fn test_validate_answers_rejects_empty_filename() {
        let questions = file_upload_questions(vec![]);
        let answers = HashMap::from([("attachment".to_string(), Answer::Single(String::new()))]);

        assert!(validate_answers(&questions, &answers).is_err());
    }

    #[test]
    fn test_validate_answers_accepts_any_file_when_accept_is_empty() {
        let questions = file_upload_questions(vec![]);
        let answers = HashMap::from([("attachment".to_string(), Answer::Single("n_importe_quoi.bin".to_string()))]);

        assert!(validate_answers(&questions, &answers).is_ok());
    }
}
