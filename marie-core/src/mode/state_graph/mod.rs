pub mod catalog;
pub mod client;
pub mod declaration;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::mode::executable::{AgentRuntime, Executable, NodeOutcome, RustRegistry};

/// Un état du graphe. `action` s'exécute à l'entrée du nœud (voir
/// [`StateGraph::execute_current`]) ; `None` pour un nœud purement de
/// contrôle (point de jonction, état terminal sans effet propre).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub action: Option<Executable>,
}

impl Node {
    #[must_use]
    pub fn new(id: impl Into<String>, action: Option<Executable>) -> Self {
        Self { id: id.into(), action }
    }
}

/// Une transition entre deux nœuds. `guard` conditionne le franchissement
/// (voir [`StateGraph::advance`]) ; `None` marque une arête par défaut,
/// empruntée si aucune arête gardée sortant du même nœud n'a matché.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    pub from: String,
    pub to: String,
    pub guard: Option<Executable>,
}

impl Edge {
    #[must_use]
    pub fn new(from: impl Into<String>, to: impl Into<String>, guard: Option<Executable>) -> Self {
        Self { from: from.into(), to: to.into(), guard }
    }
}

#[derive(Debug, Error)]
pub enum StateGraphError {
    #[error("nœud inconnu : {0}")]
    UnknownNode(String),
    #[error("l'arête '{from}' -> '{to}' référence un nœud inconnu")]
    UnknownEdgeEndpoint { from: String, to: String },
    /// Voir la note sur [`Executable::Rust`] : les variantes script ne sont
    /// pour l'instant que des données, aucun interpréteur n'est câblé.
    #[error("exécution indisponible : aucun interpréteur câblé pour ce type d'Executable")]
    UnsupportedExecutable,
    /// Le nœud courant est un [`Executable::Agent`] mais aucun
    /// [`AgentRuntime`] n'a été fourni à [`StateGraph::execute_current`] —
    /// distinct de [`Self::UnsupportedExecutable`] : l'exécution *est*
    /// câblée, elle manque seulement des clients réseau nécessaires pour
    /// joindre le control plane (voir `network::worker::mod::drive_state_graph`,
    /// qui en fournit toujours un en production).
    #[error("nœud agent rencontré sans AgentRuntime fourni")]
    MissingAgentRuntime,
    #[error("échec d'exécution : {0}")]
    ExecutionFailed(#[from] anyhow::Error),
}

/// Mode d'une session dans lequel l'exécution suit un graphe d'états
/// explicite plutôt qu'une conversation libre avec le modèle (voir
/// [`crate::mode::SessionMode::StateGraph`]) — chaque nœud peut exécuter une
/// action (voir [`Node::action`]), chaque arête peut conditionner son
/// franchissement (voir [`Edge::guard`]), les deux via un [`Executable`].
///
/// Squelette minimal, mais fonctionnel pour des nœuds/arêtes `Rust` (voir
/// [`RustRegistry`]) : `advance`/`execute_current` savent déjà piloter un tel
/// graphe. Ce qui manque est la boucle qui les invoque au fil de l'exécution
/// de l'agent (voir `agent::run`, qui ne dispatche pas encore les tool
/// calls) et l'exécution des variantes script d'[`Executable`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateGraph {
    pub nodes: HashMap<String, Node>,
    pub edges: Vec<Edge>,
    pub entry: String,
    pub current: String,
}

impl StateGraph {
    /// Construit le graphe et valide sa cohérence : `entry` et chaque
    /// extrémité d'arête doivent référencer un nœud déclaré dans `nodes` —
    /// mieux vaut le rejeter à la construction qu'échouer plus tard, en
    /// cours d'exécution, sur un nœud qui n'a jamais existé.
    pub fn new(nodes: Vec<Node>, edges: Vec<Edge>, entry: impl Into<String>) -> Result<Self, StateGraphError> {
        let entry = entry.into();
        let nodes: HashMap<String, Node> = nodes.into_iter().map(|node| (node.id.clone(), node)).collect();

        if !nodes.contains_key(&entry) {
            return Err(StateGraphError::UnknownNode(entry));
        }

        for edge in &edges {
            if !nodes.contains_key(&edge.from) || !nodes.contains_key(&edge.to) {
                return Err(StateGraphError::UnknownEdgeEndpoint { from: edge.from.clone(), to: edge.to.clone() });
            }
        }

        Ok(Self { nodes, edges, current: entry.clone(), entry })
    }

    #[must_use]
    pub fn current_node(&self) -> &Node {
        // Invariant maintenu par `Self::new` (toute arête pointe vers un
        // nœud connu) et `Self::advance` (ne fait avancer `current` que vers
        // une extrémité d'arête déjà validée) : `current` référence toujours
        // un nœud existant.
        &self.nodes[&self.current]
    }

    fn outgoing(&self, node_id: &str) -> impl Iterator<Item = &Edge> {
        self.edges.iter().filter(move |edge| edge.from == node_id)
    }

    /// Exécute l'action du nœud courant (voir [`Node::action`]), s'il y en a
    /// une — `input` est le contexte fourni tel quel à l'[`Executable`] (voir
    /// [`RustRegistry::run_node`]). Peut retourner un [`NodeOutcome::Yield`] :
    /// à l'appelant (voir `network::worker::mod::drive_state_graph`) de
    /// terminer le job sur ce yield plutôt que d'appeler [`Self::advance`].
    ///
    /// `agents` : nécessaire uniquement si le nœud courant est un
    /// [`Executable::Agent`] (voir [`run_agent_task`]) — `None` convient pour
    /// un graphe qui n'en contient pas, comme dans les tests de ce module qui
    /// n'ont pas de [`AgentRuntime`] sous la main (il faut un [`NetworkClient`](crate::network::actor::NetworkClient)
    /// connecté pour en construire un).
    pub async fn execute_current(
        &self,
        registry: &RustRegistry,
        agents: Option<&AgentRuntime>,
        input: Value,
    ) -> Result<Option<NodeOutcome>, StateGraphError> {
        match &self.current_node().action {
            None => Ok(None),
            Some(Executable::Rust { id }) => Ok(Some(registry.run_node(id, input).await?)),
            Some(Executable::Agent { expert_id, task }) => {
                let agents = agents.ok_or(StateGraphError::MissingAgentRuntime)?;
                Ok(Some(run_agent_task(agents, expert_id, task, &input).await?))
            }
            Some(Executable::Python { .. } | Executable::Rune { .. }) => Err(StateGraphError::UnsupportedExecutable),
        }
    }

    /// Évalue les arêtes sortantes du nœud courant, dans leur ordre de
    /// déclaration, et transitionne vers la première dont la garde matche
    /// (une arête sans garde matche toujours — voir [`Edge::guard`]).
    /// Retourne `false`, sans modifier [`Self::current`], si aucune arête
    /// n'a matché.
    pub async fn advance(&mut self, registry: &RustRegistry, input: Value) -> Result<bool, StateGraphError> {
        let outgoing: Vec<Edge> = self.outgoing(&self.current).cloned().collect();

        for edge in outgoing {
            let matched = match &edge.guard {
                None => true,
                Some(Executable::Rust { id }) => registry.eval_edge(id, input.clone()).await?,
                // Un agent produit une valeur (voir `run_agent_task`), pas un booléen : n'a pas
                // de sens comme garde d'arête, seulement comme action de nœud (voir `Self::execute_current`).
                Some(Executable::Agent { .. } | Executable::Python { .. } | Executable::Rune { .. }) => {
                    return Err(StateGraphError::UnsupportedExecutable);
                }
            };

            if matched {
                self.current = edge.to;
                return Ok(true);
            }
        }

        Ok(false)
    }
}

/// Exécute l'expert `expert_id` sur `task` pour un nœud [`Executable::Agent`]
/// (voir [`StateGraph::execute_current`]) : résout sa déclaration (prompt,
/// modèle, tools autorisés — voir
/// [`ExpertDeclaration`](crate::expert::declaration::ExpertDeclaration))
/// auprès du control plane, puis délègue l'appel modèle à
/// [`crate::model::execute`] — même mécanique que [`crate::agent::run`], mais
/// pilotée par le graphe plutôt que par une frame de session déjà en cours.
///
/// `input` est la valeur produite par le nœud précédent du graphe (voir
/// [`StateGraph::advance`]) : `null` au premier pas. Quand présente, elle est
/// jointe à `task` pour donner au modèle le résultat du pas précédent sans
/// que l'expert ait à le redemander — le prompt de l'expert reste, lui,
/// toujours en tête, pour que son comportement ne dépende pas de la position
/// du nœud dans le graphe.
async fn run_agent_task(agents: &AgentRuntime, expert_id: &str, task: &str, input: &Value) -> anyhow::Result<NodeOutcome> {
    let expert = agents.experts.get(expert_id).await?;
    let model = agents.model.get(expert.model_id.clone()).await?;

    let mut signatures = Vec::with_capacity(expert.allowed_tools.len());
    for tool_id in &expert.allowed_tools {
        signatures.push(agents.tools.get(tool_id.clone()).await?.signature);
    }

    let prompt = match input {
        Value::Null => format!("{}\n\n{task}", expert.prompt),
        _ => format!("{}\n\n{task}\n\nRésultat du pas précédent: {input}", expert.prompt),
    };

    let response = crate::model::execute(model, &signatures, prompt).await?;

    Ok(NodeOutcome::Value(serde_json::to_value(response)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_rejects_unknown_entry() {
        let nodes = vec![Node::new("start", None)];
        assert!(matches!(StateGraph::new(nodes, vec![], "missing"), Err(StateGraphError::UnknownNode(_))));
    }

    #[test]
    fn test_new_rejects_edge_to_unknown_node() {
        let nodes = vec![Node::new("start", None)];
        let edges = vec![Edge::new("start", "nowhere", None)];
        assert!(matches!(StateGraph::new(nodes, edges, "start"), Err(StateGraphError::UnknownEdgeEndpoint { .. })));
    }

    #[tokio::test]
    async fn test_advance_follows_default_edge() {
        let nodes = vec![Node::new("start", None), Node::new("end", None)];
        let edges = vec![Edge::new("start", "end", None)];
        let mut graph = StateGraph::new(nodes, edges, "start").unwrap();
        let registry = RustRegistry::new();

        assert!(graph.advance(&registry, Value::Null).await.unwrap());
        assert_eq!(graph.current, "end");
    }

    #[tokio::test]
    async fn test_advance_evaluates_rust_guard() {
        let nodes = vec![Node::new("start", None), Node::new("approved", None), Node::new("rejected", None)];
        let edges = vec![
            Edge::new("start", "approved", Some(Executable::Rust { id: "is_approved".to_string() })),
            Edge::new("start", "rejected", None),
        ];
        let mut graph = StateGraph::new(nodes, edges, "start").unwrap();

        let registry = RustRegistry::new();
        registry.register_edge("is_approved", |input: Value| async move { Ok(input.get("approved").and_then(Value::as_bool).unwrap_or(false)) });

        assert!(graph.advance(&registry, serde_json::json!({"approved": true})).await.unwrap());
        assert_eq!(graph.current, "approved");
    }

    #[tokio::test]
    async fn test_advance_falls_back_to_default_edge_when_guard_fails() {
        let nodes = vec![Node::new("start", None), Node::new("approved", None), Node::new("rejected", None)];
        let edges = vec![
            Edge::new("start", "approved", Some(Executable::Rust { id: "is_approved".to_string() })),
            Edge::new("start", "rejected", None),
        ];
        let mut graph = StateGraph::new(nodes, edges, "start").unwrap();

        let registry = RustRegistry::new();
        registry.register_edge("is_approved", |input: Value| async move { Ok(input.get("approved").and_then(Value::as_bool).unwrap_or(false)) });

        assert!(graph.advance(&registry, serde_json::json!({"approved": false})).await.unwrap());
        assert_eq!(graph.current, "rejected");
    }

    #[tokio::test]
    async fn test_advance_returns_false_when_no_edge_matches() {
        let nodes = vec![Node::new("start", None), Node::new("end", None)];
        let edges = vec![Edge::new("start", "end", Some(Executable::Rust { id: "never".to_string() }))];
        let mut graph = StateGraph::new(nodes, edges, "start").unwrap();

        let registry = RustRegistry::new();
        registry.register_edge("never", |_: Value| async move { Ok(false) });

        assert!(!graph.advance(&registry, Value::Null).await.unwrap());
        assert_eq!(graph.current, "start");
    }

    #[tokio::test]
    async fn test_advance_errors_on_script_guard() {
        let nodes = vec![Node::new("start", None), Node::new("end", None)];
        let edges = vec![Edge::new("start", "end", Some(Executable::Python { source: "True".to_string() }))];
        let mut graph = StateGraph::new(nodes, edges, "start").unwrap();
        let registry = RustRegistry::new();

        assert!(matches!(graph.advance(&registry, Value::Null).await, Err(StateGraphError::UnsupportedExecutable)));
    }

    #[tokio::test]
    async fn test_execute_current_runs_rust_action() {
        let nodes = vec![Node::new("start", Some(Executable::Rust { id: "greet".to_string() }))];
        let graph = StateGraph::new(nodes, vec![], "start").unwrap();

        let registry = RustRegistry::new();
        registry.register_node("greet", |_: Value| async move { Ok(NodeOutcome::Value(serde_json::json!("bonjour"))) });

        let output = graph.execute_current(&registry, None, Value::Null).await.unwrap();
        assert!(matches!(output, Some(NodeOutcome::Value(value)) if value == serde_json::json!("bonjour")));
    }

    #[tokio::test]
    async fn test_execute_current_propagates_yield() {
        let nodes = vec![Node::new("start", Some(Executable::Rust { id: "ask_human".to_string() }))];
        let graph = StateGraph::new(nodes, vec![], "start").unwrap();

        let registry = RustRegistry::new();
        let tool_call_id = crate::id::generate_id();
        registry.register_node("ask_human", move |_: Value| async move {
            Ok(NodeOutcome::Yield(crate::agent::status::YieldStatus::WaitingToolReply { tool_call_id }))
        });

        let output = graph.execute_current(&registry, None, Value::Null).await.unwrap();
        assert!(matches!(
            output,
            Some(NodeOutcome::Yield(crate::agent::status::YieldStatus::WaitingToolReply { tool_call_id: id })) if id == tool_call_id
        ));
    }

    #[tokio::test]
    async fn test_execute_current_rejects_agent_node_without_runtime() {
        let nodes = vec![Node::new(
            "start",
            Some(Executable::Agent { expert_id: "researcher".to_string(), task: "résume ce document".to_string() }),
        )];
        let graph = StateGraph::new(nodes, vec![], "start").unwrap();
        let registry = RustRegistry::new();

        let result = graph.execute_current(&registry, None, Value::Null).await;
        assert!(matches!(result, Err(StateGraphError::MissingAgentRuntime)));
    }
}
