use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, RwLock};

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    agent::{
        context::{Context, ContextEntry},
        frame::{AgentFrame, AgentFrameArgs},
        role::Role,
        status::YieldStatus,
    },
    expert::client::ExpertClient,
    hitl::Question,
    session::SessionId,
    state_graph::{Edge, Node, StateGraph, StateGraphError, client::StateGraphClient, declaration::StateGraphId},
};

/// Manière dont les enfants d'une orchestration (voir [`Executable::Orchestration`]
/// et [`crate::state_graph::orchestration::OrchestrationFrame`]) s'exécutent
/// les uns par rapport aux autres.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrchestrationStrategy {
    /// Les enfants s'exécutent l'un après l'autre : le suivant n'est soumis
    /// qu'une fois le précédent terminé.
    Sequential,
    /// Les enfants s'exécutent indépendamment ; l'orchestration ne conclut
    /// qu'une fois qu'ils ont tous terminé (AND-join).
    Parallel,
}

/// Comportement exécutable d'un nœud ou d'une arête d'un [`StateGraph`] —
/// cinq façons de le fournir, selon d'où vient la logique :
///
/// - [`Executable::Rust`] référence une fonction déjà compilée dans le
///   binaire hôte, enregistrée localement (voir [`RustRegistry`]) — le cas
///   courant pour une logique connue à la compilation du cluster.
/// - [`Executable::Python`]/[`Executable::Rune`] portent le *source* d'un
///   script, destiné à une logique définie hors du déploiement du cluster ;
///   ni l'un ni l'autre n'est câblé aujourd'hui (aucun interpréteur
///   embarqué), ce sont pour l'instant des variantes de données pures.
/// - [`Executable::Agent`] délègue le nœud à un agent du catalogue
///   d'experts (voir [`crate::expert::Expert`]) pour une tâche précise.
/// - [`Executable::Subgraph`] délègue le nœud à un graphe imbriqué —
///   composition hiérarchique, voir [`SubgraphSource`].
/// - [`Executable::Orchestration`] déclenche un fan-out de sous-tâches (voir
///   [`crate::state_graph::orchestration::OrchestrationFrame`]) sans faire
///   de l'orchestration un mode du moteur de graphe lui-même — juste un point
///   d'entrée invocable depuis un nœud.
/// - [`Executable::AskUserInput`] pousse un
///   [`crate::state_graph::hitl::HitlFrame`] et fait attendre le curseur
///   dessus (voir [`NodeOutcome::AskUserInput`]) — variante déclarative
///   (questions fixes) du tool `system/ask-user-input` côté `AgentFrame` ;
///   un nœud ayant besoin de questions construites dynamiquement à partir
///   d'une sortie précédente garde la main via un [`Executable::Rust`]
///   enregistré, dont la fonction peut renvoyer
///   [`NodeOutcome::AskUserInput`] directement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Executable {
    /// `id` : clé sous laquelle la fonction a été enregistrée (voir
    /// [`RustRegistry::register_node`]/[`RustRegistry::register_edge`]).
    /// Jamais relayée par RPC, contrairement à un tool : la logique du graphe
    /// s'exécute là où tourne déjà l'agent qui le pilote.
    Rust { id: String },
    /// `expert_id` : identifiant dans l'[`ExpertCatalog`](crate::expert::catalog::ExpertCatalog)
    /// de l'agent à exécuter. `task` est la tâche spécifique confiée à cet
    /// agent pour ce nœud, combinée au prompt de l'expert.
    Agent { expert_id: String, task: String },
    Subgraph { source: SubgraphSource },
    Orchestration { strategy: OrchestrationStrategy, children: Vec<ChildTask> },
    AskUserInput { questions: Vec<Question> },
    Python { source: String },
    Rune { source: String },
}

/// Origine d'un sous-graphe référencé par [`Executable::Subgraph`] — inline
/// (construit à la volée par l'agent qui pousse ce graphe, ex. via
/// `system/push-mode`) ou une déclaration nommée du
/// [`StateGraphCatalog`](crate::state_graph::catalog::StateGraphCatalog),
/// réutilisable d'un graphe à l'autre.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum SubgraphSource {
    Inline { nodes: Vec<Node>, edges: Vec<Edge>, entry: String },
    Catalog { id: StateGraphId },
}

/// Tâche confiée à un enfant d'[`Executable::Orchestration`] — un enfant peut
/// lui-même être un sous-graphe, pas seulement un agent nu (composition en
/// largeur *et* en profondeur).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChildTask {
    Agent { expert_id: String, task: String },
    Graph { source: SubgraphSource },
}

/// Issue de la résolution d'un [`ChildTask`] (voir
/// [`resolve_child_task`]) — un [`AgentFrame`] frais ou un [`StateGraph`]
/// frais, prêts à être insérés par l'appelant (voir
/// `session::server::push_orchestration`, qui reste la seule à muter
/// `Session` : cette fonction-ci reste pure, sans effet de bord réseau).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResolvedChildTask {
    Agent(AgentFrame),
    Graph(StateGraph),
}

/// Issue d'une fonction de nœud (voir [`RustRegistry::register_node`]) ou de
/// la résolution d'un nœud [`Executable`] par [`StateGraph::execute_current`] :
///
/// - [`NodeOutcome::Value`] : valeur produite normalement, transmise en
///   entrée du curseur suivant.
/// - [`NodeOutcome::Yield`] : demande explicite de yield (ex. un nœud `Rust`
///   qui attend une réponse humaine via [`crate::hitl`]).
/// - [`NodeOutcome::SpawnAgent`] : résolution d'un nœud [`Executable::Agent`]
///   — l'appelant (le driver `RunGraphStep`, pas cette fonction) est chargé
///   d'insérer ce frame dans `Session::frames`, de faire passer le curseur en
///   `Yielding(WaitingAgents)` **avant** de soumettre le Job `RunAgent`
///   correspondant (même ordre anti-course que `report_tool_dispatch`).
/// - [`NodeOutcome::SpawnOrchestration`] : résolution d'un nœud
///   [`Executable::Orchestration`] — même remarque, l'appelant crée
///   l'[`OrchestrationFrame`](crate::state_graph::orchestration::OrchestrationFrame).
/// - [`NodeOutcome::EnterSubgraph`] : résolution d'un nœud
///   [`Executable::Subgraph`] — l'appelant empile ce graphe sur
///   `GraphFrame::stack`.
/// - [`NodeOutcome::AskUserInput`] : résolution d'un nœud
///   [`Executable::AskUserInput`] (ou renvoyée directement par un
///   [`Executable::Rust`] enregistré) — l'appelant (`RunGraphStep`) génère
///   l'identifiant du [`crate::state_graph::hitl::HitlFrame`], fait
///   passer le curseur en `Yielding(WaitingHitl)` et persiste les deux en une
///   seule mutation (voir `session::server::push_hitl`) : cette fonction-ci
///   ne peut pas le faire elle-même (pas de `SessionClient` disponible ici,
///   voir la doc de [`GraphRuntime`]).
///
/// Volontairement pas d'attente bloquante *à l'intérieur* de la résolution
/// d'un nœud : ça figerait la tâche tokio du worker le temps de la réponse.
/// Un nœud qui a besoin d'un tiers doit retourner une des variantes
/// ci-dessus et laisser le driver terminer le job proprement.
pub enum NodeOutcome {
    Value(Value),
    Yield(YieldStatus),
    SpawnAgent(AgentFrame),
    SpawnOrchestration { strategy: OrchestrationStrategy, children: Vec<ResolvedChildTask> },
    EnterSubgraph(StateGraph),
    AskUserInput { questions: Vec<Question> },
}

/// Fonction de nœud enregistrée (voir [`RustRegistry::register_node`]) :
/// reçoit le contexte d'exécution courant (forme libre, voir
/// [`RustRegistry::run_node`]) et produit un [`NodeOutcome`].
pub type NodeFn = Arc<dyn Fn(Value) -> BoxFuture<'static, anyhow::Result<NodeOutcome>> + Send + Sync>;

/// Fonction d'arête enregistrée (voir [`RustRegistry::register_edge`]) :
/// reçoit le même contexte qu'un [`NodeFn`] et décide si l'arête doit être
/// empruntée (voir [`StateGraph::advance`]).
pub type EdgeFn = Arc<dyn Fn(Value) -> BoxFuture<'static, anyhow::Result<bool>> + Send + Sync>;

/// Fonction de routage enregistrée (voir [`RustRegistry::register_router`]) :
/// reçoit le même contexte qu'un [`NodeFn`]/[`EdgeFn`] mais, plutôt que de
/// juger une seule arête, choisit directement laquelle des arêtes sortantes
/// du nœud emprunter en renvoyant l'id du nœud cible (voir [`Node::router`]) —
/// à la charge de l'appelant ([`StateGraph::advance_cursor`]) de vérifier que
/// ce cible correspond bien à une arête sortante déclarée.
pub type RouterFn = Arc<dyn Fn(Value) -> BoxFuture<'static, anyhow::Result<String>> + Send + Sync>;

/// Registre local des fonctions Rust utilisables comme [`Executable::Rust`]
/// par les nœuds/arêtes d'un [`StateGraph`] — local au processus, donc à
/// peupler explicitement par chaque worker au démarrage, avant d'exécuter un
/// graphe qui y fait référence.
///
/// Bon marché à cloner (`Arc` interne), comme `NetworkClient`/`SessionClient`.
#[derive(Clone, Default)]
pub struct RustRegistry {
    nodes: Arc<RwLock<HashMap<String, NodeFn>>>,
    edges: Arc<RwLock<HashMap<String, EdgeFn>>>,
    routers: Arc<RwLock<HashMap<String, RouterFn>>>,
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
    /// fonction n'est enregistrée sous ce nom sur ce worker.
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

    /// Enregistre (ou remplace) la fonction de routage `id`.
    pub fn register_router<F, Fut>(&self, id: impl Into<String>, f: F)
    where
        F: Fn(Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<String>> + Send + 'static,
    {
        let f: RouterFn = Arc::new(move |input| Box::pin(f(input)));
        self.routers.write().unwrap().insert(id.into(), f);
    }

    /// Évalue la fonction de routage `id` avec `input`.
    pub async fn eval_router(&self, id: &str, input: Value) -> anyhow::Result<String> {
        let f = self.routers.read().unwrap().get(id).cloned();
        let f = f.ok_or_else(|| anyhow::anyhow!("fonction de routage inconnue : {id}"))?;
        f(input).await
    }
}

/// Clients réseau nécessaires à la résolution des nœuds
/// [`Executable::Agent`]/[`Executable::Subgraph`]/[`Executable::Orchestration`]
/// d'un [`StateGraph`] — délibérément *sans* `SessionClient`/`WorkerClient` :
/// la résolution d'un nœud (cette fonction, [`StateGraph::execute_current`])
/// reste pure et sans effet de bord sur `Session` (elle ne fait que lire des
/// catalogues et construire des valeurs), pour rester testable comme
/// aujourd'hui sans réseau live. L'insertion effective des frames enfants
/// dans `Session` et la soumission des Jobs correspondants sont la
/// responsabilité du driver (`RunGraphStep`, voir `state_graph::worker`),
/// pas de cette couche.
#[derive(Clone)]
pub struct GraphRuntime {
    pub(crate) experts: ExpertClient,
    pub(crate) state_graphs: StateGraphClient,
}

impl GraphRuntime {
    #[must_use]
    pub fn new(experts: ExpertClient, state_graphs: StateGraphClient) -> Self {
        Self { experts, state_graphs }
    }
}

/// Résout un nœud [`Executable::Agent`] en un [`AgentFrame`] frais (voir
/// [`StateGraph::execute_current`]) : `session_id` sert à construire son
/// [`AgentId`](crate::agent::AgentId), `input` est la valeur produite par le
/// curseur précédent (jointe à `task` pour donner au modèle le résultat du
/// pas précédent sans que l'expert ait à le redemander — `null` au premier
/// pas, le prompt de l'expert reste lui toujours en tête).
pub(crate) async fn resolve_agent_task(
    runtime: &GraphRuntime,
    session_id: SessionId,
    expert_id: &str,
    task: &str,
    input: &Value,
) -> Result<AgentFrame, StateGraphError> {
    let expert = runtime.experts.get(expert_id).await.map_err(|error| StateGraphError::ExecutionFailed(error.into()))?;

    let content = match input {
        Value::Null => format!("{}\n\n{task}", expert.prompt),
        _ => format!("{}\n\n{task}\n\nRésultat du pas précédent: {input}", expert.prompt),
    };

    let agent_id = crate::agent::AgentId::new(session_id, crate::id::generate_id());
    let context = Context::from(vec![ContextEntry { role: Role::User, content }]);

    Ok(AgentFrame::new(AgentFrameArgs::builder().id(agent_id).model(expert.model_id).context(context).allowed_tools(expert.allowed_tools).build()))
}

/// Résout un [`ChildTask`] (voir [`Executable::Orchestration`]) en
/// [`ResolvedChildTask`] — un agent devient un [`AgentFrame`] frais (voir
/// [`resolve_agent_task`]), un sous-graphe devient un [`StateGraph`] frais
/// (inline ou instancié depuis le catalogue, voir [`resolve_subgraph`]).
pub(crate) async fn resolve_child_task(
    runtime: &GraphRuntime,
    session_id: SessionId,
    task: &ChildTask,
) -> Result<ResolvedChildTask, StateGraphError> {
    match task {
        ChildTask::Agent { expert_id, task } => {
            Ok(ResolvedChildTask::Agent(resolve_agent_task(runtime, session_id, expert_id, task, &Value::Null).await?))
        }
        ChildTask::Graph { source } => Ok(ResolvedChildTask::Graph(resolve_subgraph(Some(runtime), source).await?)),
    }
}

/// Résout un [`SubgraphSource`] en [`StateGraph`] frais, positionné sur son
/// `entry` — [`SubgraphSource::Inline`] ne nécessite aucun réseau (fonctionne
/// même avec `runtime: None`, comme un nœud `Rust`) ; [`SubgraphSource::Catalog`]
/// nécessite un [`GraphRuntime`] pour joindre le catalogue (voir
/// [`StateGraphClient::instantiate`]).
pub(crate) async fn resolve_subgraph(runtime: Option<&GraphRuntime>, source: &SubgraphSource) -> Result<StateGraph, StateGraphError> {
    match source {
        SubgraphSource::Inline { nodes, edges, entry } => StateGraph::new(nodes.clone(), edges.clone(), entry.clone()),
        SubgraphSource::Catalog { id } => {
            let runtime = runtime.ok_or(StateGraphError::MissingGraphRuntime)?;
            runtime.state_graphs.instantiate(id.clone()).await.map_err(|error| StateGraphError::ExecutionFailed(error.into()))
        }
    }
}
