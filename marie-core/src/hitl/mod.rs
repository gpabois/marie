pub mod client;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;

use crate::{
    agent::GlobalAgentId,
    id::ID,
    tools::{
        ToolSignature,
        declaration::{ToolDeclaration, ToolScope},
    },
};

/// Identifiant, dans le [`ToolCatalog`](crate::tools::catalog::ToolCatalog),
/// du tool dédié à l'interaction humaine (voir [`tool_declaration`]) — un
/// agent qui a besoin d'une clarification ou d'une validation avant de
/// poursuivre l'appelle comme n'importe quel autre tool exposé au modèle
/// (voir [`crate::model::execute`]), avec un formulaire d'une ou plusieurs
/// [`Question`] en argument.
///
/// Contrairement à un tool ordinaire, son exécution ne passe volontairement
/// pas par [`crate::tools::client::ToolClient::call`]/`register_executor`
/// (relais RPC point-à-point, voir `network::cp::forward_race`) : ce
/// mécanisme attend une réponse dans la fenêtre du `request_response` de
/// libp2p (quelques secondes par défaut, voir `network::mod::start_swarm`),
/// bien trop courte pour le temps de réaction d'un humain. La résolution de
/// ce tool passe donc par [`client::HitlClient`], qui découple l'émission du
/// formulaire de la réception de la réponse via gossip (voir
/// [`HumanInputRequest`]/[`HumanInputAnswer`]), sans aucune limite de temps
/// imposée par le transport — seul l'appelant (voir
/// [`client::HitlClient::ask`]) peut décider d'abandonner.
pub const ASK_HUMAN_TOOL: &str = "system/ask-human";

/// Déclaration du tool [`ASK_HUMAN_TOOL`], à enregistrer une fois dans le
/// catalogue (voir [`client::HitlClient::ensure_declared`]) — portée
/// `Global` : n'importe quel agent du cluster peut solliciter un humain, ce
/// n'est pas une capacité propre à une session particulière (voir
/// [`ToolScope`]).
#[must_use]
pub fn tool_declaration() -> ToolDeclaration {
    ToolDeclaration {
        signature: ToolSignature {
            name: ASK_HUMAN_TOOL.to_string(),
            description: "Soumet un formulaire d'une ou plusieurs questions à un opérateur humain et attend ses réponses avant de \
                poursuivre. A utiliser pour lever une ambiguïté, obtenir une validation, ou recueillir une information qu'aucune \
                donnée disponible ne permet de trancher seul."
                .to_string(),
            parameters_schema: json!({
                "type": "object",
                "properties": {
                    "questions": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "properties": {
                                "key": {
                                    "type": "string",
                                    "description": "Identifiant stable de cette question (ex: \"root_cause\"), utilisé pour retrouver sa réponse dans le résultat."
                                },
                                "label": {
                                    "type": "string",
                                    "description": "Le texte de la question, présenté tel quel à l'humain."
                                },
                                "kind": {
                                    "type": "string",
                                    "enum": ["short_text", "long_text", "select", "radio", "checkboxes", "file_upload"],
                                    "description": "short_text/long_text : réponse libre (une ligne ou plusieurs). select/radio : un choix \
                                        unique parmi `options`. checkboxes : un ou plusieurs choix parmi `options`. file_upload : un \
                                        fichier à téléverser, éventuellement restreint par `accept`."
                                },
                                "options": {
                                    "type": "array",
                                    "items": { "type": "string" },
                                    "description": "Requis (et non vide) pour kind = select, radio ou checkboxes ; absent sinon."
                                },
                                "accept": {
                                    "type": "array",
                                    "items": { "type": "string" },
                                    "description": "Extensions de fichier acceptées (ex: [\".pdf\", \".png\"]), utilisé uniquement pour \
                                        kind = file_upload. Vide ou absent : tout fichier accepté."
                                }
                            },
                            "required": ["key", "label", "kind"]
                        }
                    }
                },
                "required": ["questions"]
            }),
        },
        scope: ToolScope::Global,
    }
}

/// Type de réponse attendu pour une [`Question`] — détermine à la fois le
/// composant présenté à l'humain (côté passerelle) et la forme attendue de
/// l'[`Answer`] correspondante (voir [`HumanInputRequest::validate`]).
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
    /// fichier : son contenu transite par `/session/files` dans le VFS de la
    /// session (voir [`upload_path`]), jamais par ce message gossipé — les
    /// messages `gossipsub` sont dimensionnés pour de petites charges
    /// utiles fréquentes, pas pour des fichiers.
    FileUpload { accept: Vec<String> },
}

/// Une question d'un formulaire [`HumanInputRequest`]. `key` identifie la
/// question au sein du formulaire (voir [`HumanInputAnswer::answers`]) —
/// choisie par l'appelant plutôt que générée, pour rester lisible côté
/// passerelle humaine sans avoir à conserver une correspondance question ↔
/// id (ex: `"root_cause"` plutôt qu'un [`ID`] opaque).
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
/// des choix cochés, éventuellement vide). Voir [`HumanInputRequest::validate`]
/// pour la correspondance imposée entre [`QuestionKind`] et cette forme.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Answer {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Debug, Error)]
pub enum HitlError {
    #[error("échec réseau : {0}")]
    Network(String),
    /// Aucune réponse ne viendra jamais : le [`client::HitlClient`] ayant
    /// émis la question (donc son abonnement gossip) a été détruit avant
    /// qu'une [`HumanInputAnswer`] ne soit reçue.
    #[error("plus aucun répondant possible pour ce formulaire")]
    Cancelled,
    /// La réponse soumise ne correspond pas au formulaire d'origine (voir
    /// [`HumanInputRequest::validate`]) — question manquante, type de
    /// réponse inattendu (ex: `Multiple` pour un `ShortText`), ou choix hors
    /// de `options`.
    #[error("réponse invalide : {0}")]
    InvalidAnswer(String),
}

/// Formulaire posé par un agent, diffusé sur [`client::HITL_TOPIC`] jusqu'à
/// ce qu'une passerelle humaine (voir `node::Marie::join`, typiquement une
/// passerelle HTTP/WebSocket) le prenne en charge et y réponde (voir
/// [`HumanInputAnswer`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HumanInputRequest {
    /// Corrèle la réponse à ce formulaire précis (voir
    /// [`client::HitlClient::ask`]) — plusieurs formulaires du même agent
    /// peuvent être en vol simultanément.
    pub id: ID,
    /// Agent à l'origine du formulaire — permet à la passerelle humaine de
    /// donner du contexte (quelle session, quel agent) sans avoir à le
    /// redemander.
    pub agent_id: GlobalAgentId,
    pub questions: Vec<Question>,
}

impl HumanInputRequest {
    /// Vérifie que `answers` répond à chacune des questions de ce
    /// formulaire avec une valeur du type attendu (voir [`QuestionKind`]) —
    /// à appeler côté passerelle humaine avant de publier la réponse (voir
    /// [`client::HitlClient::answer`]), pour ne jamais laisser une réponse
    /// mal formée atteindre l'agent en attente.
    pub fn validate(&self, answers: &HashMap<String, Answer>) -> Result<(), HitlError> {
        for question in &self.questions {
            let Some(answer) = answers.get(&question.key) else {
                return Err(HitlError::InvalidAnswer(format!("réponse manquante pour '{}'", question.key)));
            };

            match (&question.kind, answer) {
                (QuestionKind::ShortText | QuestionKind::LongText, Answer::Single(_)) => {}
                (QuestionKind::Select { options } | QuestionKind::Radio { options }, Answer::Single(choice)) => {
                    if !options.contains(choice) {
                        return Err(HitlError::InvalidAnswer(format!(
                            "'{choice}' n'est pas une option valide pour '{}'",
                            question.key
                        )));
                    }
                }
                (QuestionKind::Checkboxes { options }, Answer::Multiple(choices)) => {
                    if let Some(invalid) = choices.iter().find(|choice| !options.contains(choice)) {
                        return Err(HitlError::InvalidAnswer(format!(
                            "'{invalid}' n'est pas une option valide pour '{}'",
                            question.key
                        )));
                    }
                }
                (QuestionKind::FileUpload { accept }, Answer::Single(filename)) => {
                    if filename.trim().is_empty() {
                        return Err(HitlError::InvalidAnswer(format!("nom de fichier manquant pour '{}'", question.key)));
                    }
                    let accepted = accept.is_empty()
                        || accept.iter().any(|ext| filename.to_lowercase().ends_with(&ext.to_lowercase()));
                    if !accepted {
                        return Err(HitlError::InvalidAnswer(format!(
                            "'{filename}' n'a pas une extension acceptée pour '{}'",
                            question.key
                        )));
                    }
                }
                _ => {
                    return Err(HitlError::InvalidAnswer(format!("type de réponse inattendu pour '{}'", question.key)));
                }
            }
        }

        Ok(())
    }
}

/// Réponses humaines à un [`HumanInputRequest`], diffusées sur
/// [`client::HITL_TOPIC`] par la passerelle qui les a recueillies (voir
/// [`client::HitlClient::answer`]) — une entrée par [`Question::key`] du
/// formulaire d'origine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HumanInputAnswer {
    pub request_id: ID,
    pub answers: HashMap<String, Answer>,
}

/// Chemin, au sein de `/session/files` dans le VFS de la session de l'agent
/// (voir `persistency::vfs::WorkspaceVfs`, accessible via
/// `session::client::SessionClient::read_file`/`write_file`),
/// où la passerelle humaine doit écrire le contenu d'un fichier téléversé
/// (voir [`QuestionKind::FileUpload`]) avant de publier sa réponse, et où
/// l'agent doit le relire une fois celle-ci reçue.
///
/// Dérivé plutôt que transmis tel quel dans [`HumanInputAnswer`], pour que
/// l'agent n'ait jamais à faire confiance à un chemin arbitraire fourni par
/// un pair — `key` et `filename` sont réduits à leur dernier segment (voir
/// [`sanitize_path_segment`]) pour qu'aucun des deux ne puisse faire sortir
/// le résultat du préfixe `hitl/{request_id}/`.
#[must_use]
pub fn upload_path(request_id: ID, key: &str, filename: &str) -> String {
    format!("hitl/{request_id}/{}/{}", sanitize_path_segment(key), sanitize_path_segment(filename))
}

/// Réduit `segment` à son dernier composant de chemin (voir [`upload_path`]) —
/// une entrée vide, `.` ou `..` (qui ne désignerait rien d'utile une fois
/// isolée) est remplacée par `_` plutôt que de produire un chemin invalide
/// ou ambigu.
fn sanitize_path_segment(segment: &str) -> String {
    match segment.rsplit(['/', '\\']).next() {
        Some(candidate) if !candidate.is_empty() && candidate != "." && candidate != ".." => candidate.to_string(),
        _ => "_".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request() -> HumanInputRequest {
        HumanInputRequest {
            id: crate::id::generate_id(),
            agent_id: GlobalAgentId::new(crate::id::generate_id(), crate::id::generate_id()),
            questions: vec![
                Question::short_text("name", "Votre nom ?"),
                Question::select("env", "Environnement ?", vec!["prod".into(), "staging".into()]),
                Question::checkboxes("notify", "Notifier qui ?", vec!["slack".into(), "email".into()]),
            ],
        }
    }

    #[test]
    fn test_validate_accepts_matching_answers() {
        let request = sample_request();
        let answers = HashMap::from([
            ("name".to_string(), Answer::Single("Ada".to_string())),
            ("env".to_string(), Answer::Single("prod".to_string())),
            ("notify".to_string(), Answer::Multiple(vec!["slack".to_string()])),
        ]);

        assert!(request.validate(&answers).is_ok());
    }

    #[test]
    fn test_validate_rejects_missing_answer() {
        let request = sample_request();
        let answers = HashMap::from([("name".to_string(), Answer::Single("Ada".to_string()))]);

        assert!(request.validate(&answers).is_err());
    }

    #[test]
    fn test_validate_rejects_choice_outside_options() {
        let request = sample_request();
        let answers = HashMap::from([
            ("name".to_string(), Answer::Single("Ada".to_string())),
            ("env".to_string(), Answer::Single("dev".to_string())),
            ("notify".to_string(), Answer::Multiple(vec![])),
        ]);

        assert!(request.validate(&answers).is_err());
    }

    #[test]
    fn test_validate_rejects_wrong_answer_shape() {
        let request = sample_request();
        let answers = HashMap::from([
            ("name".to_string(), Answer::Multiple(vec!["Ada".to_string()])),
            ("env".to_string(), Answer::Single("prod".to_string())),
            ("notify".to_string(), Answer::Multiple(vec![])),
        ]);

        assert!(request.validate(&answers).is_err());
    }

    fn file_upload_request(accept: Vec<String>) -> HumanInputRequest {
        HumanInputRequest {
            id: crate::id::generate_id(),
            agent_id: GlobalAgentId::new(crate::id::generate_id(), crate::id::generate_id()),
            questions: vec![Question::file_upload("attachment", "Joindre un fichier", accept)],
        }
    }

    #[test]
    fn test_validate_accepts_matching_file_extension() {
        let request = file_upload_request(vec![".pdf".to_string(), ".png".to_string()]);
        let answers = HashMap::from([("attachment".to_string(), Answer::Single("rapport.PDF".to_string()))]);

        assert!(request.validate(&answers).is_ok());
    }

    #[test]
    fn test_validate_rejects_unaccepted_file_extension() {
        let request = file_upload_request(vec![".pdf".to_string()]);
        let answers = HashMap::from([("attachment".to_string(), Answer::Single("photo.jpg".to_string()))]);

        assert!(request.validate(&answers).is_err());
    }

    #[test]
    fn test_validate_rejects_empty_filename() {
        let request = file_upload_request(vec![]);
        let answers = HashMap::from([("attachment".to_string(), Answer::Single(String::new()))]);

        assert!(request.validate(&answers).is_err());
    }

    #[test]
    fn test_validate_accepts_any_file_when_accept_is_empty() {
        let request = file_upload_request(vec![]);
        let answers = HashMap::from([("attachment".to_string(), Answer::Single("n_importe_quoi.bin".to_string()))]);

        assert!(request.validate(&answers).is_ok());
    }

    #[test]
    fn test_upload_path_confines_traversal_attempts() {
        let id = crate::id::generate_id();
        let path = upload_path(id, "attachment", "../../etc/passwd");

        assert_eq!(path, format!("hitl/{id}/attachment/passwd"));
    }

    #[test]
    fn test_upload_path_replaces_empty_or_dot_segments() {
        let id = crate::id::generate_id();

        assert_eq!(upload_path(id, "attachment", "../"), format!("hitl/{id}/attachment/_"));
        assert_eq!(upload_path(id, "attachment", ".."), format!("hitl/{id}/attachment/_"));
    }
}
