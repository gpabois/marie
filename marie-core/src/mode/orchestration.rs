use serde::{Deserialize, Serialize};

use crate::agent::GlobalAgentId;

/// Manière dont les enfants d'une [`Orchestration`] s'exécutent les uns par
/// rapport aux autres.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrchestrationStrategy {
    /// Les enfants s'exécutent l'un après l'autre, chacun voyant le résultat
    /// du précédent (voir [`Orchestration::children`]) — utile quand une
    /// étape dépend du résultat de la précédente.
    Sequential,
    /// Les enfants s'exécutent indépendamment ; l'orchestrateur reprend la
    /// main une fois qu'ils ont tous terminé.
    Parallel,
}

/// Mode d'une session dans lequel un agent orchestrateur délègue une partie
/// de son travail à des agents enfants plutôt que de tout traiter seul (voir
/// [`crate::mode::SessionMode::Orchestration`]).
///
/// Squelette minimal : ne porte que la stratégie de coordination et la liste
/// des enfants déjà créés — la boucle qui décide effectivement quand créer
/// un enfant, attend sa complétion ou agrège ses résultats dépend de la
/// boucle d'exécution de l'agent (voir `agent::run`), qui ne dispatche pas
/// encore les tool calls qu'elle reçoit et n'est donc pas encore en mesure
/// de piloter ce mode. Cette structure fixe la forme des données que cette
/// boucle consommera le jour où elle existe, sans figer sa logique.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Orchestration {
    pub strategy: OrchestrationStrategy,
    /// Agents enfants de cette orchestration, dans l'ordre de création (voir
    /// [`Self::add_child`]).
    pub children: Vec<GlobalAgentId>,
}

impl Orchestration {
    #[must_use]
    pub fn new(strategy: OrchestrationStrategy) -> Self {
        Self { strategy, children: Vec::new() }
    }

    pub fn add_child(&mut self, agent_id: GlobalAgentId) {
        self.children.push(agent_id);
    }
}
