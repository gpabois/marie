use serde::{Deserialize, Serialize};

use crate::{expert::ExpertId, model::ModelId, tools::ToolId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShellMode {
    LoopModel {
        system_prompt: String,
        model_id: ModelId,
        allowed_tools: Vec<ToolId>
    },
    /// Boucle de discussion pilotée par un expert du catalogue (voir
    /// [`crate::expert::Expert`]) plutôt qu'un modèle nu — le prompt système,
    /// le modèle et les outils autorisés sont résolus à partir de
    /// `expert_id` au moment de l'exécution, pas fournis ici : contrairement
    /// à [`Self::LoopModel`], l'appelant n'a besoin de connaître que l'id de
    /// l'expert.
    Expert(ExpertId)
}