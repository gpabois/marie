pub mod catalog;
pub mod client;
pub mod declaration;
pub mod executable;
pub mod frame;
pub mod hitl;
pub mod orchestration;
pub mod rpc;
pub mod server;
pub mod worker;

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::{
    agent::status::AgentStatus,
    session::{
        SessionId,
        state::executable::{Executable, GraphRuntime, NodeOutcome, resolve_agent_task, resolve_child_task, resolve_subgraph},
    },
};

pub const NS_STATE_GRAPH: &str = "/marie/ns/state-graphs";

/// Rôle d'un nœud dans le franchissement de ses arêtes (voir
/// [`StateGraph::advance_cursor`]) :
///
/// - [`NodeKind::Step`] (défaut) : comportement d'origine — une seule arête
///   sortante est empruntée (première garde qui matche, ou l'arête par
///   défaut).
/// - [`NodeKind::Fork`] : *toutes* les arêtes sortantes sont empruntées à la
///   fois, un nouveau [`Cursor`] par arête (elles doivent être non gardées,
///   voir [`StateGraphError::GuardedForkEdge`]) — parallélisme topologique
///   déclaré dans la structure même du graphe (voir la doc du module).
/// - [`NodeKind::Join`] : rendez-vous — un curseur qui atteint ce nœud est
///   parqué jusqu'à ce que tous les curseurs attendus (arité = nombre
///   d'arêtes entrantes, voir [`StateGraphError::JoinArityTooLow`]) soient
///   arrivés, puis fusionnés en un seul.
/// - [`NodeKind::Start`] : marqueur explicite du point d'entrée du graphe, en
///   plus du champ [`StateGraph::entry`] déjà porté par le graphe — au plus
///   un par graphe (voir [`StateGraphError::MultipleStartNodes`]), et doit
///   coïncider avec `entry` (voir [`StateGraphError::EntryNotStartNode`]) :
///   c'est une annotation de lisibilité/validation, pas un mécanisme
///   alternatif de sélection de l'entrée. Ne peut porter aucune arête
///   entrante (voir [`StateGraphError::StartNodeHasIncomingEdges`]).
/// - [`NodeKind::End`] : marqueur explicite d'un point de sortie — un nœud
///   sans arête sortante conclut déjà un curseur (voir
///   [`AdvanceOutcome::Terminal`]) que son `kind` soit [`NodeKind::End`] ou
///   non ; ce kind ne fait que rendre l'intention explicite dans la
///   déclaration, en échange de quoi il est interdit de lui donner une arête
///   sortante (voir [`StateGraphError::EndNodeHasOutgoingEdges`]).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    #[default]
    Step,
    Fork,
    Join,
    Start,
    End,
}

/// Un état du graphe. `action` s'exécute à l'entrée du nœud (voir
/// [`StateGraph::execute_cursor`]) ; `None` pour un nœud purement de
/// contrôle (point de jonction, état terminal sans effet propre).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub action: Option<Executable>,
    /// Nœud cible si `action` échoue (voir [`StateGraph::execute_cursor`]),
    /// au lieu de faire échouer tout le curseur.
    pub on_error: Option<String>,
    pub kind: NodeKind,
    /// Choix explicite, à l'entrée dans [`StateGraph::advance_cursor`], de
    /// l'arête sortante à emprunter parmi celles déclarées sur ce nœud —
    /// évalué une seule fois avec `last_output` du curseur et doit renvoyer
    /// l'id du nœud cible d'une des arêtes sortantes (voir
    /// [`StateGraphError::RouterTargetNotOutgoing`]), plutôt que de laisser
    /// chaque arête juger indépendamment via `Edge::guard`. `None` (défaut)
    /// conserve le comportement d'origine : la première arête gardée qui
    /// matche (ou l'arête par défaut) est retenue. Un nœud portant un
    /// `router` ne doit déclarer aucune arête sortante gardée (voir
    /// [`StateGraphError::GuardedEdgeWithRouter`], sur le même principe que
    /// [`StateGraphError::GuardedForkEdge`]) : les deux mécanismes de choix
    /// ne se combinent pas. Incompatible avec [`NodeKind::Fork`] (voir
    /// [`StateGraphError::RouterOnForkNode`]), qui emprunte déjà *toutes* ses
    /// arêtes sortantes sans en choisir une seule.
    #[serde(default)]
    pub router: Option<Executable>,
}

impl Node {
    #[must_use]
    pub fn new(id: impl Into<String>, action: Option<Executable>) -> Self {
        Self { id: id.into(), action, on_error: None, kind: NodeKind::Step, router: None }
    }

    #[must_use]
    pub fn fork(id: impl Into<String>) -> Self {
        Self { id: id.into(), action: None, on_error: None, kind: NodeKind::Fork, router: None }
    }

    #[must_use]
    pub fn join(id: impl Into<String>, action: Option<Executable>) -> Self {
        Self { id: id.into(), action, on_error: None, kind: NodeKind::Join, router: None }
    }

    #[must_use]
    pub fn start(id: impl Into<String>) -> Self {
        Self { id: id.into(), action: None, on_error: None, kind: NodeKind::Start, router: None }
    }

    #[must_use]
    pub fn end(id: impl Into<String>, action: Option<Executable>) -> Self {
        Self { id: id.into(), action, on_error: None, kind: NodeKind::End, router: None }
    }

    #[must_use]
    pub fn with_on_error(mut self, node_id: impl Into<String>) -> Self {
        self.on_error = Some(node_id.into());
        self
    }

    #[must_use]
    pub fn with_router(mut self, router: Executable) -> Self {
        self.router = Some(router);
        self
    }
}

/// Une transition entre deux nœuds. `guard` conditionne le franchissement
/// (voir [`StateGraph::advance_cursor`]) ; `None` marque une arête par
/// défaut, empruntée si aucune arête gardée sortant du même nœud n'a matché.
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
    #[error("le nœud '{0}' déclare on_error vers un nœud inconnu : {1}")]
    UnknownErrorTarget(String, String),
    #[error("le nœud Join '{0}' a une arité < 2 (au moins deux arêtes entrantes attendues)")]
    JoinArityTooLow(String),
    #[error("le nœud Fork '{0}' a une arête sortante gardée : les arêtes sortantes d'un Fork doivent être inconditionnelles")]
    GuardedForkEdge(String),
    #[error("le nœud Fork '{0}' ne peut pas porter de router : un Fork emprunte déjà toutes ses arêtes sortantes")]
    RouterOnForkNode(String),
    #[error("plusieurs nœuds Start déclarés : '{0}' et '{1}' — un seul point d'entrée est autorisé par graphe")]
    MultipleStartNodes(String, String),
    #[error("l'entrée du graphe ('{entry}') doit être le nœud Start déclaré ('{start}')")]
    EntryNotStartNode { entry: String, start: String },
    #[error("le nœud Start '{0}' a une arête entrante : un point d'entrée ne peut être ciblé par aucune arête")]
    StartNodeHasIncomingEdges(String),
    #[error("le nœud End '{0}' a une arête sortante : un point de sortie ne peut avoir aucune arête sortante")]
    EndNodeHasOutgoingEdges(String),
    #[error("le nœud '{0}' porte un router et une arête sortante gardée : les deux mécanismes de choix ne se combinent pas")]
    GuardedEdgeWithRouter(String),
    #[error("le router du nœud '{node}' a choisi '{target}', qui n'est pas une arête sortante de ce nœud")]
    RouterTargetNotOutgoing { node: String, target: String },
    #[error("curseur inconnu : {0}")]
    UnknownCursor(CursorId),
    /// Voir la note sur [`Executable::Rust`] : les variantes script ne sont
    /// pour l'instant que des données, aucun interpréteur n'est câblé.
    #[error("exécution indisponible : aucun interpréteur câblé pour ce type d'Executable")]
    UnsupportedExecutable,
    /// Le nœud courant est un [`Executable::Agent`]/[`Executable::Orchestration`]
    /// ou référence un [`executable::SubgraphSource::Catalog`], mais aucun
    /// [`GraphRuntime`] n'a été fourni — distinct de [`Self::UnsupportedExecutable`] :
    /// l'exécution *est* câblée, elle manque seulement des clients réseau
    /// nécessaires pour joindre les catalogues.
    #[error("nœud agent/sous-graphe/orchestration rencontré sans GraphRuntime fourni")]
    MissingGraphRuntime,
    #[error("échec d'exécution : {0}")]
    ExecutionFailed(#[from] anyhow::Error),
}

/// Identifiant d'un [`Cursor`] — un simple compteur local au
/// [`StateGraph`]/[`crate::session::state::frame::GraphStackFrame`] qui le
/// porte : pas besoin d'unicité globale (contrairement à [`crate::agent::AgentId`]),
/// deux curseurs de deux graphes différents peuvent porter le même id sans
/// collision, seule l'unicité *au sein d'un même* `StateGraph` compte.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorId(u32);

impl fmt::Display for CursorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Position d'exécution dans un [`StateGraph`] — en régime normal, un
/// `StateGraph` porte un seul curseur ; un nœud [`NodeKind::Fork`] le
/// remplace par plusieurs curseurs indépendants (voir
/// [`StateGraph::advance_cursor`]), chacun avec son propre statut :
/// contrairement à l'ancien modèle à curseur unique, un curseur peut yielder
/// (ex. nœud `Agent`) sans bloquer la progression de ses frères issus du même
/// `Fork`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cursor {
    pub id: CursorId,
    pub current: String,
    pub last_output: Value,
    pub status: AgentStatus,
}

/// Issue de [`StateGraph::advance_cursor`].
#[derive(Debug, Clone, PartialEq)]
pub enum AdvanceOutcome {
    /// Le curseur a franchi une arête normale.
    Moved,
    /// Le curseur (nœud [`NodeKind::Fork`]) a été remplacé par `.0` nouveaux
    /// curseurs indépendants.
    Forked(usize),
    /// Le curseur est arrivé à un nœud [`NodeKind::Join`] mais n'est pas
    /// encore rejoint par tous les curseurs attendus — retiré de
    /// [`StateGraph::cursors`], parqué dans [`StateGraph::joins`].
    ParkedAtJoin,
    /// Le dernier curseur attendu par un `Join` vient d'arriver : tous les
    /// curseurs parqués pour ce nœud ont été fusionnés en un seul (sorties
    /// agrégées en `Value::Array`), réinséré dans [`StateGraph::cursors`].
    Joined,
    /// Le curseur a atteint un nœud sans arête sortante : il conclut
    /// (`status` passe à [`AgentStatus::Finished`]).
    Terminal,
    /// Aucune arête sortante ne matche (garde échouée sans arête par
    /// défaut) — le curseur reste sur son nœud courant.
    NoMatch,
}

/// Mode d'exécution d'une session dans lequel le déroulement suit un graphe
/// d'états explicite (voir [`crate::session::state::frame::GraphFrame`]) —
/// chaque nœud peut exécuter une action (voir [`Node::action`]), chaque
/// arête peut conditionner son franchissement (voir [`Edge::guard`]), un
/// nœud peut router vers l'un des trois modes de composition décrits dans la
/// doc de [`NodeKind`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateGraph {
    pub nodes: HashMap<String, Node>,
    pub edges: Vec<Edge>,
    pub entry: String,
    pub cursors: Vec<Cursor>,
    /// Curseurs parqués en attente d'un rendez-vous (voir
    /// [`AdvanceOutcome::ParkedAtJoin`]), par id de nœud `Join`.
    pub joins: HashMap<String, Vec<Cursor>>,
    next_cursor_id: u32,
}

impl StateGraph {
    /// Construit le graphe et valide sa cohérence : `entry` et chaque
    /// extrémité d'arête/`on_error` doivent référencer un nœud déclaré dans
    /// `nodes`, chaque nœud `Join` doit avoir au moins deux arêtes entrantes,
    /// chaque arête sortante d'un nœud `Fork` doit être inconditionnelle —
    /// mieux vaut rejeter à la construction qu'échouer plus tard, en cours
    /// d'exécution, sur une incohérence de topologie.
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

        let mut start_nodes = nodes.values().filter(|node| node.kind == NodeKind::Start).map(|node| node.id.as_str());
        if let Some(start) = start_nodes.next() {
            if let Some(other_start) = start_nodes.next() {
                return Err(StateGraphError::MultipleStartNodes(start.to_string(), other_start.to_string()));
            }
            if start != entry {
                return Err(StateGraphError::EntryNotStartNode { entry: entry.clone(), start: start.to_string() });
            }
        }

        for node in nodes.values() {
            if let Some(on_error) = &node.on_error
                && !nodes.contains_key(on_error)
            {
                return Err(StateGraphError::UnknownErrorTarget(node.id.clone(), on_error.clone()));
            }

            match node.kind {
                NodeKind::Join => {
                    let arity = edges.iter().filter(|edge| edge.to == node.id).count();
                    if arity < 2 {
                        return Err(StateGraphError::JoinArityTooLow(node.id.clone()));
                    }
                }
                NodeKind::Fork => {
                    let has_guarded_edge = edges.iter().any(|edge| edge.from == node.id && edge.guard.is_some());
                    if has_guarded_edge {
                        return Err(StateGraphError::GuardedForkEdge(node.id.clone()));
                    }
                    if node.router.is_some() {
                        return Err(StateGraphError::RouterOnForkNode(node.id.clone()));
                    }
                }
                NodeKind::Start => {
                    if edges.iter().any(|edge| edge.to == node.id) {
                        return Err(StateGraphError::StartNodeHasIncomingEdges(node.id.clone()));
                    }
                }
                NodeKind::End => {
                    if edges.iter().any(|edge| edge.from == node.id) {
                        return Err(StateGraphError::EndNodeHasOutgoingEdges(node.id.clone()));
                    }
                }
                NodeKind::Step => {}
            }

            if node.router.is_some() {
                let has_guarded_edge = edges.iter().any(|edge| edge.from == node.id && edge.guard.is_some());
                if has_guarded_edge {
                    return Err(StateGraphError::GuardedEdgeWithRouter(node.id.clone()));
                }
            }
        }

        let cursors = vec![Cursor { id: CursorId(0), current: entry.clone(), last_output: Value::Null, status: AgentStatus::Running }];

        Ok(Self { nodes, edges, entry, cursors, joins: HashMap::new(), next_cursor_id: 1 })
    }

    #[must_use]
    pub fn cursor(&self, id: CursorId) -> Option<&Cursor> {
        self.cursors.iter().find(|cursor| cursor.id == id)
    }

    /// Premier curseur prêt à avancer (`Running`) — pratique pour un graphe
    /// sans `Fork` actif, où un seul curseur existe à la fois.
    #[must_use]
    pub fn ready_cursor(&self) -> Option<&Cursor> {
        self.cursors.iter().find(|cursor| cursor.status == AgentStatus::Running)
    }

    fn cursor_index(&self, id: CursorId) -> Result<usize, StateGraphError> {
        self.cursors.iter().position(|cursor| cursor.id == id).ok_or(StateGraphError::UnknownCursor(id))
    }

    fn outgoing(&self, node_id: &str) -> impl Iterator<Item = &Edge> {
        self.edges.iter().filter(move |edge| edge.from == node_id)
    }

    fn incoming_count(&self, node_id: &str) -> usize {
        self.edges.iter().filter(|edge| edge.to == node_id).count()
    }

    fn alloc_cursor_id(&mut self) -> CursorId {
        let id = CursorId(self.next_cursor_id);
        self.next_cursor_id += 1;
        id
    }

    /// Exécute l'action du nœud où se trouve le curseur `cursor_id` (voir
    /// [`Node::action`]), s'il y en a une, contre son `last_output` courant.
    /// Peut retourner un [`NodeOutcome::Yield`]/[`NodeOutcome::SpawnAgent`]/
    /// [`NodeOutcome::SpawnOrchestration`]/[`NodeOutcome::EnterSubgraph`] : à
    /// l'appelant (le driver `RunGraphStep`, voir [`crate::session::state::worker`])
    /// de traiter chacun sans appeler [`Self::advance_cursor`] dans la
    /// foulée — voir la doc de [`NodeOutcome`].
    ///
    /// Sur un [`NodeOutcome::Value`], met à jour `last_output` du curseur.
    /// Sur un [`NodeOutcome::Yield`]/[`NodeOutcome::SpawnAgent`], met à jour
    /// son `status`. Sur une erreur d'exécution, redirige vers [`Node::on_error`]
    /// si déclaré (le curseur reste actif, positionné sur le nœud d'erreur,
    /// `last_output` porte le message d'erreur) plutôt que de faire remonter
    /// l'erreur — sauf si `on_error` est absent, auquel cas elle remonte
    /// telle quelle (le curseur, lui, n'est pas modifié).
    ///
    /// `session_id` : nécessaire pour construire l'[`crate::agent::AgentId`]
    /// d'un éventuel [`AgentFrame`](crate::agent::frame::AgentFrame) enfant
    /// (nœuds `Agent`/`Orchestration`).
    pub async fn execute_cursor(
        &mut self,
        cursor_id: CursorId,
        registry: &executable::RustRegistry,
        runtime: Option<&GraphRuntime>,
        session_id: SessionId,
    ) -> Result<Option<NodeOutcome>, StateGraphError> {
        let index = self.cursor_index(cursor_id)?;
        let node_id = self.cursors[index].current.clone();
        let input = self.cursors[index].last_output.clone();
        let node = self.nodes.get(&node_id).ok_or_else(|| StateGraphError::UnknownNode(node_id.clone()))?.clone();

        let result: Result<Option<NodeOutcome>, StateGraphError> = match &node.action {
            None => Ok(None),
            Some(Executable::Rust { id }) => registry.run_node(id, input).await.map(Some).map_err(StateGraphError::ExecutionFailed),
            Some(Executable::Agent { expert_id, task }) => {
                let runtime = runtime.ok_or(StateGraphError::MissingGraphRuntime)?;
                resolve_agent_task(runtime, session_id, expert_id, task, &input).await.map(|frame| Some(NodeOutcome::SpawnAgent(frame)))
            }
            Some(Executable::Orchestration { strategy, children }) => {
                let runtime = runtime.ok_or(StateGraphError::MissingGraphRuntime)?;
                let mut resolved = Vec::with_capacity(children.len());
                let mut failure = None;
                for child in children {
                    match resolve_child_task(runtime, session_id, child).await {
                        Ok(task) => resolved.push(task),
                        Err(error) => {
                            failure = Some(error);
                            break;
                        }
                    }
                }
                match failure {
                    Some(error) => Err(error),
                    None => Ok(Some(NodeOutcome::SpawnOrchestration { strategy: *strategy, children: resolved })),
                }
            }
            Some(Executable::Subgraph { source }) => {
                resolve_subgraph(runtime, source).await.map(|graph| Some(NodeOutcome::EnterSubgraph(graph)))
            }
            Some(Executable::AskUserInput { questions }) => Ok(Some(NodeOutcome::AskUserInput { questions: questions.clone() })),
            Some(Executable::Python { .. } | Executable::Rune { .. }) => Err(StateGraphError::UnsupportedExecutable),
        };

        match result {
            Ok(outcome) => {
                match &outcome {
                    Some(NodeOutcome::Value(value)) => self.cursors[index].last_output = value.clone(),
                    Some(NodeOutcome::Yield(status)) => self.cursors[index].status = AgentStatus::Yielding(status.clone()),
                    Some(NodeOutcome::SpawnAgent(frame)) => {
                        self.cursors[index].status =
                            AgentStatus::Yielding(crate::agent::status::YieldStatus::WaitingAgents { agents: vec![frame.id] });
                    }
                    Some(NodeOutcome::SpawnOrchestration { .. } | NodeOutcome::EnterSubgraph(_) | NodeOutcome::AskUserInput { .. }) | None => {}
                }
                Ok(outcome)
            }
            Err(error) if node.on_error.is_some() => {
                let on_error = node.on_error.unwrap();
                self.cursors[index].current = on_error;
                self.cursors[index].last_output = serde_json::json!({ "error": error.to_string() });
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Détermine, puis franchit, l'arête sortante du nœud où se trouve le
    /// curseur `cursor_id` — sauf pour un nœud [`NodeKind::Fork`], dont
    /// *toutes* les arêtes sortantes sont empruntées à la fois (voir
    /// [`AdvanceOutcome::Forked`]). Deux mécanismes de choix, mutuellement
    /// exclusifs (voir [`Node::router`]) :
    ///
    /// - [`Node::router`] présent : évalué une seule fois avec `last_output`
    ///   du curseur, doit renvoyer l'id du nœud cible d'une des arêtes
    ///   sortantes du nœud courant (voir
    ///   [`StateGraphError::RouterTargetNotOutgoing`] sinon) — les
    ///   `Edge::guard` des arêtes sortantes ne sont pas consultées (rejetées
    ///   dès [`StateGraph::new`], voir [`StateGraphError::GuardedEdgeWithRouter`]).
    /// - [`Node::router`] absent (défaut) : les arêtes sortantes sont
    ///   évaluées dans leur ordre de déclaration, la première dont la garde
    ///   matche est retenue (une arête sans garde matche toujours — voir
    ///   [`Edge::guard`]) ; si aucune ne matche, [`AdvanceOutcome::NoMatch`].
    ///
    /// Si l'arête choisie mène à un nœud [`NodeKind::Join`], le curseur est
    /// parqué (voir [`AdvanceOutcome::ParkedAtJoin`]) plutôt que d'avancer
    /// directement.
    pub async fn advance_cursor(&mut self, cursor_id: CursorId, registry: &executable::RustRegistry) -> Result<AdvanceOutcome, StateGraphError> {
        let index = self.cursor_index(cursor_id)?;
        let node_id = self.cursors[index].current.clone();
        let outgoing: Vec<Edge> = self.outgoing(&node_id).cloned().collect();

        if outgoing.is_empty() {
            self.cursors[index].status = AgentStatus::Finished;
            return Ok(AdvanceOutcome::Terminal);
        }

        if self.nodes[&node_id].kind == NodeKind::Fork {
            let cursor = self.cursors.remove(index);
            let mut created = 0usize;
            for edge in &outgoing {
                let id = self.alloc_cursor_id();
                self.cursors.push(Cursor { id, current: edge.to.clone(), last_output: cursor.last_output.clone(), status: AgentStatus::Running });
                created += 1;
            }
            return Ok(AdvanceOutcome::Forked(created));
        }

        let input = self.cursors[index].last_output.clone();

        let target = if let Some(router) = self.nodes[&node_id].router.clone() {
            let chosen = match &router {
                Executable::Rust { id } => registry.eval_router(id, input).await.map_err(StateGraphError::ExecutionFailed)?,
                Executable::Agent { .. }
                | Executable::Orchestration { .. }
                | Executable::Subgraph { .. }
                | Executable::AskUserInput { .. }
                | Executable::Python { .. }
                | Executable::Rune { .. } => {
                    return Err(StateGraphError::UnsupportedExecutable);
                }
            };

            if !outgoing.iter().any(|edge| edge.to == chosen) {
                return Err(StateGraphError::RouterTargetNotOutgoing { node: node_id.clone(), target: chosen });
            }

            chosen
        } else {
            let mut target = None;
            for edge in &outgoing {
                let matched = match &edge.guard {
                    None => true,
                    Some(Executable::Rust { id }) => registry.eval_edge(id, input.clone()).await.map_err(StateGraphError::ExecutionFailed)?,
                    Some(
                        Executable::Agent { .. }
                        | Executable::Orchestration { .. }
                        | Executable::Subgraph { .. }
                        | Executable::AskUserInput { .. }
                        | Executable::Python { .. }
                        | Executable::Rune { .. },
                    ) => {
                        return Err(StateGraphError::UnsupportedExecutable);
                    }
                };

                if matched {
                    target = Some(edge.to.clone());
                    break;
                }
            }

            let Some(target) = target else { return Ok(AdvanceOutcome::NoMatch) };
            target
        };

        if self.nodes[&target].kind == NodeKind::Join {
            let cursor = self.cursors.remove(index);
            let arrived = self.joins.entry(target.clone()).or_default();
            arrived.push(cursor);

            if arrived.len() < self.incoming_count(&target) {
                return Ok(AdvanceOutcome::ParkedAtJoin);
            }

            let arrived = self.joins.remove(&target).unwrap();
            let merged_output = Value::Array(arrived.into_iter().map(|cursor| cursor.last_output).collect());
            let id = self.alloc_cursor_id();
            self.cursors.push(Cursor { id, current: target, last_output: merged_output, status: AgentStatus::Running });
            return Ok(AdvanceOutcome::Joined);
        }

        self.cursors[index].current = target;
        Ok(AdvanceOutcome::Moved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::state::executable::RustRegistry;

    fn session_id() -> SessionId {
        SessionId::new(crate::id::generate_id())
    }

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

    #[test]
    fn test_new_rejects_on_error_to_unknown_node() {
        let nodes = vec![Node::new("start", None).with_on_error("nowhere")];
        assert!(matches!(StateGraph::new(nodes, vec![], "start"), Err(StateGraphError::UnknownErrorTarget(_, _))));
    }

    #[test]
    fn test_new_rejects_join_with_arity_below_two() {
        let nodes = vec![Node::new("start", None), Node::join("rendezvous", None)];
        let edges = vec![Edge::new("start", "rendezvous", None)];
        assert!(matches!(StateGraph::new(nodes, edges, "start"), Err(StateGraphError::JoinArityTooLow(_))));
    }

    #[test]
    fn test_new_rejects_guarded_fork_edge() {
        let nodes = vec![Node::fork("split"), Node::new("a", None)];
        let edges = vec![Edge::new("split", "a", Some(Executable::Rust { id: "always".to_string() }))];
        assert!(matches!(StateGraph::new(nodes, edges, "split"), Err(StateGraphError::GuardedForkEdge(_))));
    }

    #[test]
    fn test_new_rejects_router_on_fork_node() {
        let nodes =
            vec![Node::fork("split").with_router(Executable::Rust { id: "pick".to_string() }), Node::new("a", None), Node::new("b", None)];
        let edges = vec![Edge::new("split", "a", None), Edge::new("split", "b", None)];
        assert!(matches!(StateGraph::new(nodes, edges, "split"), Err(StateGraphError::RouterOnForkNode(_))));
    }

    #[test]
    fn test_new_rejects_guarded_edge_with_router() {
        let nodes = vec![
            Node::new("start", None).with_router(Executable::Rust { id: "pick".to_string() }),
            Node::new("a", None),
            Node::new("b", None),
        ];
        let edges = vec![Edge::new("start", "a", Some(Executable::Rust { id: "always".to_string() })), Edge::new("start", "b", None)];
        assert!(matches!(StateGraph::new(nodes, edges, "start"), Err(StateGraphError::GuardedEdgeWithRouter(_))));
    }

    #[test]
    fn test_new_rejects_multiple_start_nodes() {
        let nodes = vec![Node::start("a"), Node::start("b"), Node::new("c", None)];
        let edges = vec![Edge::new("a", "c", None), Edge::new("b", "c", None)];
        assert!(matches!(StateGraph::new(nodes, edges, "a"), Err(StateGraphError::MultipleStartNodes(_, _))));
    }

    #[test]
    fn test_new_rejects_entry_not_matching_start_node() {
        let nodes = vec![Node::start("start"), Node::new("other", None)];
        assert!(matches!(StateGraph::new(nodes, vec![], "other"), Err(StateGraphError::EntryNotStartNode { .. })));
    }

    #[test]
    fn test_new_rejects_start_node_with_incoming_edges() {
        let nodes = vec![Node::new("a", None), Node::start("start")];
        let edges = vec![Edge::new("a", "start", None)];
        assert!(matches!(StateGraph::new(nodes, edges, "start"), Err(StateGraphError::StartNodeHasIncomingEdges(_))));
    }

    #[test]
    fn test_new_rejects_end_node_with_outgoing_edges() {
        let nodes = vec![Node::new("a", None), Node::end("end", None)];
        let edges = vec![Edge::new("end", "a", None)];
        assert!(matches!(StateGraph::new(nodes, edges, "a"), Err(StateGraphError::EndNodeHasOutgoingEdges(_))));
    }

    #[tokio::test]
    async fn test_advance_marks_end_node_terminal() {
        let nodes = vec![Node::start("start"), Node::end("end", None)];
        let edges = vec![Edge::new("start", "end", None)];
        let mut graph = StateGraph::new(nodes, edges, "start").unwrap();
        let registry = RustRegistry::new();
        let cursor_id = graph.cursors[0].id;

        assert_eq!(graph.advance_cursor(cursor_id, &registry).await.unwrap(), AdvanceOutcome::Moved);
        assert_eq!(graph.cursors[0].current, "end");
        assert_eq!(graph.advance_cursor(cursor_id, &registry).await.unwrap(), AdvanceOutcome::Terminal);
        assert_eq!(graph.cursors[0].status, AgentStatus::Finished);
    }

    #[tokio::test]
    async fn test_advance_follows_default_edge() {
        let nodes = vec![Node::new("start", None), Node::new("end", None)];
        let edges = vec![Edge::new("start", "end", None)];
        let mut graph = StateGraph::new(nodes, edges, "start").unwrap();
        let registry = RustRegistry::new();
        let cursor_id = graph.cursors[0].id;

        assert_eq!(graph.advance_cursor(cursor_id, &registry).await.unwrap(), AdvanceOutcome::Moved);
        assert_eq!(graph.cursors[0].current, "end");
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

        let cursor_id = graph.cursors[0].id;
        graph.cursors[0].last_output = serde_json::json!({"approved": true});
        assert_eq!(graph.advance_cursor(cursor_id, &registry).await.unwrap(), AdvanceOutcome::Moved);
        assert_eq!(graph.cursors[0].current, "approved");
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

        let cursor_id = graph.cursors[0].id;
        graph.cursors[0].last_output = serde_json::json!({"approved": false});
        assert_eq!(graph.advance_cursor(cursor_id, &registry).await.unwrap(), AdvanceOutcome::Moved);
        assert_eq!(graph.cursors[0].current, "rejected");
    }

    #[tokio::test]
    async fn test_advance_uses_router_to_pick_target() {
        let nodes = vec![
            Node::new("start", None).with_router(Executable::Rust { id: "pick".to_string() }),
            Node::new("a", None),
            Node::new("b", None),
            Node::new("c", None),
        ];
        let edges = vec![Edge::new("start", "a", None), Edge::new("start", "b", None), Edge::new("start", "c", None)];
        let mut graph = StateGraph::new(nodes, edges, "start").unwrap();

        let registry = RustRegistry::new();
        registry.register_router("pick", |input: Value| async move {
            Ok(input.get("choice").and_then(Value::as_str).unwrap_or("a").to_string())
        });

        let cursor_id = graph.cursors[0].id;
        graph.cursors[0].last_output = serde_json::json!({"choice": "c"});
        assert_eq!(graph.advance_cursor(cursor_id, &registry).await.unwrap(), AdvanceOutcome::Moved);
        assert_eq!(graph.cursors[0].current, "c");
    }

    #[tokio::test]
    async fn test_advance_router_errors_when_target_not_outgoing() {
        let nodes =
            vec![Node::new("start", None).with_router(Executable::Rust { id: "pick".to_string() }), Node::new("a", None), Node::new("elsewhere", None)];
        let edges = vec![Edge::new("start", "a", None)];
        let mut graph = StateGraph::new(nodes, edges, "start").unwrap();

        let registry = RustRegistry::new();
        registry.register_router("pick", |_: Value| async move { Ok("elsewhere".to_string()) });

        let cursor_id = graph.cursors[0].id;
        let result = graph.advance_cursor(cursor_id, &registry).await;
        assert!(matches!(result, Err(StateGraphError::RouterTargetNotOutgoing { .. })));
    }

    #[tokio::test]
    async fn test_advance_returns_no_match_when_no_edge_matches() {
        let nodes = vec![Node::new("start", None), Node::new("end", None)];
        let edges = vec![Edge::new("start", "end", Some(Executable::Rust { id: "never".to_string() }))];
        let mut graph = StateGraph::new(nodes, edges, "start").unwrap();

        let registry = RustRegistry::new();
        registry.register_edge("never", |_: Value| async move { Ok(false) });

        let cursor_id = graph.cursors[0].id;
        assert_eq!(graph.advance_cursor(cursor_id, &registry).await.unwrap(), AdvanceOutcome::NoMatch);
        assert_eq!(graph.cursors[0].current, "start");
    }

    #[tokio::test]
    async fn test_advance_marks_terminal_node_finished() {
        let nodes = vec![Node::new("start", None)];
        let mut graph = StateGraph::new(nodes, vec![], "start").unwrap();
        let registry = RustRegistry::new();
        let cursor_id = graph.cursors[0].id;

        assert_eq!(graph.advance_cursor(cursor_id, &registry).await.unwrap(), AdvanceOutcome::Terminal);
        assert_eq!(graph.cursors[0].status, AgentStatus::Finished);
    }

    #[tokio::test]
    async fn test_advance_errors_on_script_guard() {
        let nodes = vec![Node::new("start", None), Node::new("end", None)];
        let edges = vec![Edge::new("start", "end", Some(Executable::Python { source: "True".to_string() }))];
        let mut graph = StateGraph::new(nodes, edges, "start").unwrap();
        let registry = RustRegistry::new();
        let cursor_id = graph.cursors[0].id;

        assert!(matches!(graph.advance_cursor(cursor_id, &registry).await, Err(StateGraphError::UnsupportedExecutable)));
    }

    #[tokio::test]
    async fn test_execute_current_runs_rust_action() {
        let nodes = vec![Node::new("start", Some(Executable::Rust { id: "greet".to_string() }))];
        let mut graph = StateGraph::new(nodes, vec![], "start").unwrap();

        let registry = RustRegistry::new();
        registry.register_node("greet", |_: Value| async move { Ok(NodeOutcome::Value(serde_json::json!("bonjour"))) });

        let cursor_id = graph.cursors[0].id;
        let output = graph.execute_cursor(cursor_id, &registry, None, session_id()).await.unwrap();
        assert!(matches!(output, Some(NodeOutcome::Value(value)) if value == serde_json::json!("bonjour")));
        assert_eq!(graph.cursors[0].last_output, serde_json::json!("bonjour"));
    }

    #[tokio::test]
    async fn test_execute_current_propagates_yield() {
        let nodes = vec![Node::new("start", Some(Executable::Rust { id: "ask_human".to_string() }))];
        let mut graph = StateGraph::new(nodes, vec![], "start").unwrap();

        let registry = RustRegistry::new();
        let tools_calls = vec![crate::tools::ToolCallId::new(session_id(), crate::id::generate_id())];
        let expected = tools_calls.clone();
        registry.register_node("ask_human", move |_: Value| {
            let tools_calls = tools_calls.clone();
            async move {
                Ok(NodeOutcome::Yield(crate::agent::status::YieldStatus::WaitingToolReply {
                    tools_calls,
                    tools_outputs: Default::default(),
                }))
            }
        });

        let cursor_id = graph.cursors[0].id;
        let output = graph.execute_cursor(cursor_id, &registry, None, session_id()).await.unwrap();
        assert!(matches!(
            output,
            Some(NodeOutcome::Yield(crate::agent::status::YieldStatus::WaitingToolReply { tools_calls, .. })) if tools_calls == expected
        ));
    }

    #[tokio::test]
    async fn test_execute_current_rejects_agent_node_without_runtime() {
        let nodes = vec![Node::new("start", Some(Executable::Agent { expert_id: "researcher".to_string(), task: "résume ce document".to_string() }))];
        let mut graph = StateGraph::new(nodes, vec![], "start").unwrap();
        let registry = RustRegistry::new();
        let cursor_id = graph.cursors[0].id;

        let result = graph.execute_cursor(cursor_id, &registry, None, session_id()).await;
        assert!(matches!(result, Err(StateGraphError::MissingGraphRuntime)));
    }

    #[tokio::test]
    async fn test_execute_current_on_error_redirects_instead_of_failing() {
        let nodes = vec![
            Node::new("start", Some(Executable::Rust { id: "boom".to_string() })).with_on_error("recover"),
            Node::new("recover", None),
        ];
        let mut graph = StateGraph::new(nodes, vec![], "start").unwrap();
        let registry = RustRegistry::new();
        registry.register_node("boom", |_: Value| async move { Err(anyhow::anyhow!("échec simulé")) });

        let cursor_id = graph.cursors[0].id;
        let outcome = graph.execute_cursor(cursor_id, &registry, None, session_id()).await.unwrap();
        assert!(outcome.is_none());
        assert_eq!(graph.cursors[0].current, "recover");
        assert_eq!(graph.cursors[0].last_output["error"], serde_json::json!("échec d'exécution : échec simulé"));
    }

    #[tokio::test]
    async fn test_execute_current_without_on_error_propagates_failure() {
        let nodes = vec![Node::new("start", Some(Executable::Rust { id: "boom".to_string() }))];
        let mut graph = StateGraph::new(nodes, vec![], "start").unwrap();
        let registry = RustRegistry::new();
        registry.register_node("boom", |_: Value| async move { Err(anyhow::anyhow!("échec simulé")) });

        let cursor_id = graph.cursors[0].id;
        let result = graph.execute_cursor(cursor_id, &registry, None, session_id()).await;
        assert!(matches!(result, Err(StateGraphError::ExecutionFailed(_))));
    }

    #[tokio::test]
    async fn test_execute_current_enters_inline_subgraph_without_runtime() {
        let inner_nodes = vec![Node::new("inner_start", None)];
        let nodes = vec![Node::new(
            "start",
            Some(Executable::Subgraph {
                source: executable::SubgraphSource::Inline { nodes: inner_nodes, edges: vec![], entry: "inner_start".to_string() },
            }),
        )];
        let mut graph = StateGraph::new(nodes, vec![], "start").unwrap();
        let registry = RustRegistry::new();
        let cursor_id = graph.cursors[0].id;

        let outcome = graph.execute_cursor(cursor_id, &registry, None, session_id()).await.unwrap();
        assert!(matches!(outcome, Some(NodeOutcome::EnterSubgraph(inner)) if inner.entry == "inner_start"));
    }

    #[tokio::test]
    async fn test_execute_current_catalog_subgraph_requires_runtime() {
        let nodes = vec![Node::new(
            "start",
            Some(Executable::Subgraph { source: executable::SubgraphSource::Catalog { id: declaration::StateGraphId::new("reusable") } }),
        )];
        let mut graph = StateGraph::new(nodes, vec![], "start").unwrap();
        let registry = RustRegistry::new();
        let cursor_id = graph.cursors[0].id;

        let result = graph.execute_cursor(cursor_id, &registry, None, session_id()).await;
        assert!(matches!(result, Err(StateGraphError::MissingGraphRuntime)));
    }

    #[tokio::test]
    async fn test_fork_creates_one_cursor_per_outgoing_edge() {
        let nodes = vec![Node::fork("split"), Node::new("a", None), Node::new("b", None)];
        let edges = vec![Edge::new("split", "a", None), Edge::new("split", "b", None)];
        let mut graph = StateGraph::new(nodes, edges, "split").unwrap();
        let registry = RustRegistry::new();
        let cursor_id = graph.cursors[0].id;

        let outcome = graph.advance_cursor(cursor_id, &registry).await.unwrap();
        assert_eq!(outcome, AdvanceOutcome::Forked(2));
        assert_eq!(graph.cursors.len(), 2);
        let mut targets: Vec<&str> = graph.cursors.iter().map(|cursor| cursor.current.as_str()).collect();
        targets.sort_unstable();
        assert_eq!(targets, vec!["a", "b"]);
    }

    #[tokio::test]
    async fn test_join_waits_for_all_branches_before_advancing() {
        let nodes = vec![Node::fork("split"), Node::new("a", None), Node::new("b", None), Node::join("rendezvous", None)];
        let edges = vec![
            Edge::new("split", "a", None),
            Edge::new("split", "b", None),
            Edge::new("a", "rendezvous", None),
            Edge::new("b", "rendezvous", None),
        ];
        let mut graph = StateGraph::new(nodes, edges, "split").unwrap();
        let registry = RustRegistry::new();

        let root_id = graph.cursors[0].id;
        graph.advance_cursor(root_id, &registry).await.unwrap();
        assert_eq!(graph.cursors.len(), 2);

        let first_id = graph.cursors[0].id;
        let second_id = graph.cursors[1].id;

        let outcome = graph.advance_cursor(first_id, &registry).await.unwrap();
        assert_eq!(outcome, AdvanceOutcome::ParkedAtJoin);
        assert_eq!(graph.cursors.len(), 1, "le curseur parqué est retiré de `cursors`, pas encore rejoint");

        let outcome = graph.advance_cursor(second_id, &registry).await.unwrap();
        assert_eq!(outcome, AdvanceOutcome::Joined);
        assert_eq!(graph.cursors.len(), 1);
        assert_eq!(graph.cursors[0].current, "rendezvous");
        assert!(graph.joins.is_empty());
    }
}
