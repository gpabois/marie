use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, RwLock};

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    agent::status::YieldStatus,
    expert::client::ExpertClient,
    hitl::client::HitlClient,
    model::ModelClient,
    network::actor::NetworkClient,
    tools::client::ToolClient,
};

/// Comportement exécutable d'un nœud ou d'une arête d'un
/// [`crate::mode::state_graph::StateGraph`] — quatre façons de le fournir,
/// selon d'où vient la logique :
///
/// - [`Executable::Rust`] référence une fonction déjà compilée dans le
///   binaire hôte, enregistrée localement (voir [`RustRegistry`]) — le cas
///   courant pour une logique connue à la compilation du cluster.
/// - [`Executable::Python`]/[`Executable::Rune`] portent le *source* d'un
///   script, destiné à une logique définie hors du déploiement du cluster
///   (ex: configurée par un opérateur, sans recompilation) — pensées pour un
///   worker qui embarquerait un interpréteur.
/// - [`Executable::Agent`] délègue le nœud à un agent du catalogue
///   d'experts (voir [`crate::expert::declaration::ExpertDeclaration`]),
///   chargé d'une tâche précise plutôt que d'exécuter du code — contrairement
///   aux trois autres variantes, sa logique n'est pas locale au worker : elle
///   consulte le control plane (résolution de l'expert, du modèle, des
///   tools), voir [`AgentRuntime`].
///
/// Seuls `Rust` et `Agent` sont exécutables aujourd'hui : `marie-core` ne
/// dépend d'aucun interpréteur (pas de `pyo3` ni de `rune` à ce stade). Les
/// variantes script ne sont pour l'instant que des données — voir
/// `state_graph::StateGraphError::UnsupportedExecutable` côté appelant. Ce
/// découpage en variantes, choisi dès ce squelette, évite d'avoir à revoir la
/// structure des graphes/orchestrations qui les référencent le jour où un de
/// ces moteurs est effectivement câblé.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Executable {
    /// `id` : clé sous laquelle la fonction a été enregistrée (voir
    /// [`RustRegistry::register_node`]/[`RustRegistry::register_edge`]).
    /// Contrairement à un tool (voir `tools::client::ToolClient::call`),
    /// jamais relayé par RPC à un autre pair : doit être enregistrée sur
    /// chaque worker susceptible d'exécuter ce nœud/cette arête, comme un
    /// `match` explicite plutôt qu'un exécuteur distant — la logique du
    /// graphe s'exécute là où tourne déjà l'agent, pas ailleurs.
    Rust { id: String },
    Python { source: String },
    Rune { source: String },
    /// `expert_id` : identifiant dans l'[`ExpertCatalog`](crate::expert::catalog::ExpertCatalog)
    /// de l'agent à exécuter (son prompt/modèle/tools autorisés sont résolus
    /// au moment de l'exécution, pas ici — voir
    /// [`crate::expert::declaration::ExpertDeclaration`]). `task` est la
    /// tâche spécifique confiée à cet agent pour ce nœud, combinée au prompt
    /// de l'expert (voir `state_graph::run_agent_task`) : c'est ce qui
    /// distingue un nœud `Agent` d'un simple appel de fonction — l'expert est
    /// réutilisable tel quel d'un graphe à l'autre, seule la tâche change.
    Agent { expert_id: String, task: String },
}

/// Issue d'une fonction de nœud (voir [`RustRegistry::register_node`]) —
/// soit une valeur produite normalement (transmise en entrée de
/// `state_graph::StateGraph::advance`, puis du nœud suivant), soit une
/// demande explicite de yield.
///
/// Volontairement pas d'attente bloquante *à l'intérieur* d'un [`NodeFn`]
/// (ex: appeler `crate::hitl::client::HitlClient::ask` directement et
/// attendre sa résolution) : ça figerait la tâche tokio du worker le temps
/// de la réponse, exactement ce que le transport gossip de [`crate::hitl`] a
/// été pensé pour éviter. Un nœud qui a besoin d'un humain doit retourner
/// `Yield(WaitingToolReply { .. })` et laisser la boucle de pilotage (voir
/// `network::worker::mod::drive_state_graph`) terminer le job proprement —
/// la reprise, une fois la réponse arrivée, se fait sur un nouveau job (voir
/// `network::cp::mod::resume_after_hitl_answer`), pas en débloquant celui-ci.
#[derive(Debug)]
pub enum NodeOutcome {
    Value(Value),
    Yield(YieldStatus),
}

/// Fonction de nœud enregistrée (voir [`RustRegistry::register_node`]) :
/// reçoit le contexte d'exécution courant (forme libre, voir
/// [`RustRegistry::run_node`]) et produit un [`NodeOutcome`].
pub type NodeFn = Arc<dyn Fn(Value) -> BoxFuture<'static, anyhow::Result<NodeOutcome>> + Send + Sync>;

/// Fonction d'arête enregistrée (voir [`RustRegistry::register_edge`]) :
/// reçoit le même contexte qu'un [`NodeFn`] et décide si l'arête doit être
/// empruntée (voir `state_graph::StateGraph::advance`).
pub type EdgeFn = Arc<dyn Fn(Value) -> BoxFuture<'static, anyhow::Result<bool>> + Send + Sync>;

/// Registre local des fonctions Rust utilisables comme [`Executable::Rust`]
/// par les nœuds/arêtes d'un [`crate::mode::state_graph::StateGraph`] —
/// local au processus (voir la note sur [`Executable::Rust`]), donc à
/// peupler explicitement par chaque worker au démarrage (ex: dans
/// `network::worker::start_worker`), avant d'exécuter un graphe qui y fait
/// référence.
///
/// Bon marché à cloner (`Arc` interne), comme `NetworkClient`/`SessionClient`.
#[derive(Clone, Default)]
pub struct RustRegistry {
    nodes: Arc<RwLock<HashMap<String, NodeFn>>>,
    edges: Arc<RwLock<HashMap<String, EdgeFn>>>,
}

impl RustRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Enregistre (ou remplace) la fonction de nœud `id`.
    pub fn register_node<F, Fut>(&self, id: impl Into<String>, f: F)
    where
        F: Fn(Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<NodeOutcome>> + Send + 'static,
    {
        let f: NodeFn = Arc::new(move |input| Box::pin(f(input)));
        self.nodes.write().unwrap().insert(id.into(), f);
    }

    /// Enregistre (ou remplace) la fonction d'arête `id`.
    pub fn register_edge<F, Fut>(&self, id: impl Into<String>, f: F)
    where
        F: Fn(Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<bool>> + Send + 'static,
    {
        let f: EdgeFn = Arc::new(move |input| Box::pin(f(input)));
        self.edges.write().unwrap().insert(id.into(), f);
    }

    /// Exécute la fonction de nœud `id` avec `input` — échoue si aucune
    /// fonction n'est enregistrée sous ce nom sur ce worker (voir la note
    /// sur [`Executable::Rust`] : rien à relayer, l'appelant doit
    /// l'enregistrer localement au préalable).
    pub async fn run_node(&self, id: &str, input: Value) -> anyhow::Result<NodeOutcome> {
        let f = self.nodes.read().unwrap().get(id).cloned();
        let f = f.ok_or_else(|| anyhow::anyhow!("fonction de nœud inconnue : {id}"))?;
        f(input).await
    }

    /// Évalue la fonction d'arête `id` avec `input`.
    pub async fn eval_edge(&self, id: &str, input: Value) -> anyhow::Result<bool> {
        let f = self.edges.read().unwrap().get(id).cloned();
        let f = f.ok_or_else(|| anyhow::anyhow!("fonction d'arête inconnue : {id}"))?;
        f(input).await
    }
}

/// Clients réseau nécessaires à l'exécution d'un agent — que ce soit un nœud
/// [`Executable::Agent`] d'un `StateGraph` (voir `state_graph::run_agent_task`)
/// ou un agent en mode [`crate::mode::SessionMode::Simple`] (voir
/// `network::worker::mod::run_simple`, qui délègue à [`crate::agent::run`]).
/// Contrairement à [`RustRegistry`] (fonctions déjà compilées, purement
/// locales), les deux passent par le control plane (catalogues
/// d'experts/modèles/tools) et, pour `agent::run`, par le transport gossip
/// de [`crate::hitl`]. Bon marché à cloner, comme les [`NetworkClient`] qu'il
/// regroupe.
#[derive(Clone)]
pub struct AgentRuntime {
    pub(crate) experts: ExpertClient,
    pub(crate) model: ModelClient,
    pub(crate) tools: ToolClient,
    pub(crate) hitl: HitlClient,
}

impl AgentRuntime {
    #[must_use]
    pub fn new(client: NetworkClient) -> Self {
        Self {
            experts: ExpertClient::new(client.clone()),
            model: ModelClient::new(client.clone()),
            tools: ToolClient::new(client.clone()),
            hitl: HitlClient::new(client),
        }
    }
}
