use std::hash::Hash;
use std::{borrow::Borrow, collections::HashMap, fmt};

use serde::{Deserialize, Serialize};

use crate::catalog::{CatalogItemRef, Catalogable};
use crate::graph::{NodeId, NodeSpec};
use crate::session::spec::CommonSpec;

pub trait GraphState: Clone {}

/// Identifiant unique d'une déclaration de [`Graph`] dans le catalogue (voir
/// [`crate::graph::Graphs`]) — même forme que
/// [`crate::expert::ExpertId`]/[`crate::model::ModelId`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct GraphId(String);

impl GraphId {
    pub fn new(id: impl ToString) -> Self {
        Self(id.to_string())
    }
}

impl fmt::Display for GraphId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for GraphId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

impl From<&str> for GraphId {
    fn from(id: &str) -> Self {
        Self(id.to_owned())
    }
}

impl Borrow<str> for GraphId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl AsRef<[u8]> for GraphId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GraphRef(pub(crate) CatalogItemRef);

impl GraphRef {
    /// Accès à la [`CatalogItemRef`] sous-jacente pour
    /// [`crate::graph::Graphs::get`] — `graph::mod` (module parent de
    /// celui-ci) n'a pas accès au champ privé de `GraphRef` par simple
    /// portée de module, contrairement à `GraphSpecRef` qui y est défini
    /// directement.
    pub(crate) fn catalog_ref(&self) -> &CatalogItemRef {
        &self.0
    }
}



/// Déclaration d'un graphe (nodes/edges/point d'entrée), publiée dans le
/// catalogue via [`crate::graph::Graphs`] — sur le même modèle que
/// [`crate::expert::Expert`] : ne référence ses nodes que par [`NodeId`]
/// (résolues à l'exécution via le registre natif tenu par
/// [`crate::graph::Graphs`], voir [`crate::graph::node::Nodable::register`]),
/// donc ne porte aucun secret, contrairement à
/// [`crate::model::EncryptedModel`].
///
/// # Forme
///
/// - [`Self::nodes`] : chaque [`NodeSpec`] référence un *type* de node natif
///   par son [`NodeKindId`](crate::graph::NodeKindId) (voir
///   [`crate::graph::node::Nodable::kind_id`]) — jamais le comportement
///   directement, celui-ci vit dans le registre en mémoire tenu par
///   [`crate::graph::Graphs`] (voir [`Graphs::register`](crate::graph::Graphs::register)),
///   peuplé au démarrage de chaque worker. `NodeSpec::overrides` fixe/écrase
///   les canaux hérités propres à *cette* instance du node (voir
///   [`crate::graph::node::NodeContext`]) — deux nœuds du même
///   [`NodeKindId`] dans le même graphe (ou dans deux graphes différents)
///   peuvent avoir des `overrides` complètement différents.
/// - [`Self::edges`] : une simple correspondance `NodeId -> NodeId`, sans
///   condition ni garde — un graphe qui a besoin de brancher selon l'état
///   courant le fait via un node [`crate::graph::script::GraphScriptEngine`]
///   qui appelle `goto(...)` (voir sa doc, notamment l'exemple de routeur
///   conditionnel), pas via une structure déclarative de `edges`.
///
/// # Exemple — deux nodes natifs, dont un routeur scripté
///
/// Enchaîne un node `execute-script` ([`crate::graph::builtin::ExecuteScript`])
/// qui route selon `review_status`, vers un node `ask-expert`
/// ([`crate::graph::builtin::AskExpert`]) quand une nouvelle relecture est
/// nécessaire — les deux sont des nodes natifs fournis par ce crate (voir
/// `graph::builtin`), enregistrés comme n'importe quel [`Nodable`] tiers,
/// pas un mécanisme spécial.
///
/// ```
/// use marie_core::graph::{GraphId, GraphSpec, NodeSpec};
/// use marie_core::session::spec::CommonSpec;
///
/// const ROUTER_SCRIPT: &str = r#"
///     pub fn main(ctx) {
///         let status = ctx.read("review_status")?;
///         match status {
///             "changes_requested" => goto("ask-reviewer")?,
///             "approved" => completed()?,
///             _ => next()?,
///         }
///     }
/// "#;
///
/// let mut graph = GraphSpec::new(GraphId::new("code-review"), "route", CommonSpec::default());
///
/// graph.add_node("route".into(), NodeSpec::execute_script(ROUTER_SCRIPT));
/// graph.add_node("ask-reviewer".into(), NodeSpec::ask_expert("senior-reviewer", "relire ce changement"));
///
/// // Fixe (voir plus haut) : une fois relu, `ask-reviewer` repasse par
/// // `route` pour réévaluer `review_status`.
/// graph.edges.insert("ask-reviewer".into(), "route".into());
/// ```
///
/// [`NodeSpec::execute_script`] et [`NodeSpec::ask_expert`] sont les
/// constructeurs dédiés à ces deux nodes natifs (voir `graph::builtin`) —
/// équivalents à construire le [`NodeSpec`] à la main avec
/// [`ExecuteScript::kind_id`](crate::graph::builtin::ExecuteScript::kind_id)/
/// [`::common_spec`](crate::graph::builtin::ExecuteScript::common_spec) (ou
/// leurs équivalents [`AskExpert`](crate::graph::builtin::AskExpert)), sans
/// avoir à s'y répéter à chaque graphe qui les utilise.
#[derive(Clone, Serialize, Deserialize)]
pub struct GraphSpec {
    pub id: GraphId,
    pub nodes: HashMap<NodeId, NodeSpec>,
    pub edges: HashMap<NodeId, NodeId>,
    pub entry: NodeId,
    pub common: CommonSpec
}

impl GraphSpec {
    pub fn new(id: GraphId, entry: impl Into<NodeId>, common: CommonSpec) -> Self {
        Self {
            id,
            nodes: HashMap::new(),
            edges: HashMap::new(),
            entry: entry.into(),
            common
        }
    }

    pub fn add_node(&mut self, id: impl Into<NodeId>, params: NodeSpec) {
        self.nodes.insert(id.into(), params);
    }

    pub fn add_edge(&mut self, src: impl Into<NodeId>, dest: impl Into<NodeId>) {
        self.edges.insert(src.into(), dest.into());
    }
}

impl Catalogable for GraphSpec {
    const KIND: &str = "/marie/catalog/graphs";

    fn id(&self) -> &str {
        self.id.borrow()
    }
}
