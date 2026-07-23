use std::collections::HashMap;

use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};

use crate::{
    hitl::{Answer, Question},
    id::ID,
    session::SessionId,
    state_graph::orchestration::Waiter,
};

/// Identifiant d'un [`HitlFrame`], scopé à sa session — même forme que
/// [`crate::agent::AgentId`]/[`crate::state_graph::frame::GraphFrameId`]/
/// [`crate::state_graph::orchestration::OrchestrationFrameId`].
#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct HitlFrameId(SessionId, ID);

impl HitlFrameId {
    pub fn new(session_id: SessionId, id: ID) -> Self {
        Self(session_id, id)
    }

    pub fn session_id(&self) -> SessionId {
        self.0
    }

    pub fn local_id(&self) -> ID {
        self.1
    }
}

impl AsRef<[u8]> for HitlFrameId {
    fn as_ref(&self) -> &[u8] {
        bytemuck::bytes_of(self)
    }
}

impl std::fmt::Display for HitlFrameId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.0, self.1)
    }
}

impl std::str::FromStr for HitlFrameId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (session_part, local_part) = s.split_once('/').ok_or_else(|| anyhow::anyhow!("format de HitlFrameId invalide : {s}"))?;
        Ok(Self(session_part.parse()?, local_part.parse()?))
    }
}

/// Sérialisé/désérialisé comme une chaîne plutôt que via le `derive` par
/// défaut — même raison que [`crate::agent::AgentId`] : sert de clé de
/// `HashMap` dans [`crate::session::model::Session::hitls`], sérialisé en
/// JSON par [`crate::session::catalog::SessionCatalog`].
impl Serialize for HitlFrameId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for HitlFrameId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let repr = String::deserialize(deserializer)?;
        repr.parse().map_err(serde::de::Error::custom)
    }
}

/// État d'un [`HitlFrame`] : `Pending` tant qu'aucune réponse n'est arrivée,
/// `Answered` une fois [`crate::session::server::report_user_input`] appelé
/// avec succès — les réponses sont conservées ici plutôt que d'être
/// immédiatement effacées, pour que l'appelant puisse encore consulter ce
/// qui a été répondu (ex. une passerelle humaine qui veut confirmer sa propre
/// soumission) et pour que [`crate::session::server::report_user_input`]
/// puisse détecter un rejeu (voir sa doc pour l'idempotence).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum HitlFrameStatus {
    Pending,
    Answered { answers: HashMap<String, Answer> },
}

/// Formulaire humain, satellite d'un [`crate::agent::frame::AgentFrame`] ou
/// d'un [`crate::state_graph::frame::GraphFrame`] au même titre qu'une
/// [`crate::state_graph::orchestration::OrchestrationFrame`] (voir la doc
/// de [`crate::state_graph`]) — poussé par le tool `system/ask-user-input`
/// (voir [`crate::tools::builtin::ASK_USER_INPUT_TOOL`]) ou par un nœud
/// [`crate::state_graph::executable::Executable::AskUserInput`] d'un
/// `StateGraph`. `owner` réutilise [`Waiter`] tel quel : il joue ici
/// exactement le même rôle que pour `OrchestrationFrame::owner`, "qui a
/// poussé ceci et attend dessus".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HitlFrame {
    pub id: HitlFrameId,
    pub owner: Waiter,
    pub questions: Vec<Question>,
    pub status: HitlFrameStatus,
}
