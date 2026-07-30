use std::{collections::HashMap, fmt, ops::{Deref, DerefMut}, str::FromStr, sync::Arc};

use moka::future::Cache;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use typed_builder::TypedBuilder;

use crate::{
    graph::{Graphs, NodeId, graph::GraphRef}, 
    id::{ID, generate_id}, 
    job::JobState, 
    session::{
        Session, 
        SessionId, 
        channel::ChannelName, 
        controller::SessionError, 
        protocol::FrameResponse, 
        snapshot::SnapshotRef, 
        spec::CommonSpec, 
        store::SessionStore
    }, 
    store::PgStore
};


#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameId(ID);

impl FrameId {
    pub fn new() -> Self {
        Self(generate_id())
    }
}

impl fmt::Display for FrameId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for FrameId {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrameStatus {
    #[default]
    Pending,
    RunCompleted(FrameResponse),
    RunFailed(String),
    Failed(String),
    Completed,
    Ready,
    Running(JobState),
    RunExhausted,
    WaitingChildren
}

impl FrameStatus {
    pub fn has_terminated(&self) -> bool {
        matches!(self, FrameStatus::Failed(_) | FrameStatus::Completed)
    }

    pub fn has_completed(&self) -> bool {
        matches!(self, FrameStatus::Completed)
    }

    pub fn has_failed(&self) -> bool {
        matches!(self, FrameStatus::Failed(_))
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrameData {
    #[default]
    Void,
    Graph {
        graph_ref: GraphRef,
    },
    GraphNode {
        graph_ref: GraphRef,
        node_id: NodeId
    },
    GraphThread {
        graph_ref: GraphRef,
    },
    GraphSpan {
        graph_ref: GraphRef,
        join: NodeId
    }
}

#[derive(TypedBuilder)]
pub struct NewFrameNodeArgs {
    session_id: SessionId,
    spec_ref: FrameSpecRef,
    data: FrameData,
    #[builder(default)]
    inherited_channels: HashMap<ChannelName, Value>,
    #[builder(default, setter(strip_option))]
    forked_from: Option<SnapshotRef>,
    #[builder(default)]
    frame_policy: FramePolicy,
    #[builder(default)]
    barrier: bool,
    #[builder(default)]
    superstep: u32
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FramePolicy {
    pub child_failure_policy: ChildFailurePolicy,
    pub parent_policy: ParentPolicy
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChildFailurePolicy {
    FailIfAtLeastHasFailed(usize),
    #[default]
    DontFail
}

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParentPolicy {
    #[default]
    // Reduce only on the last terminated child frame
    Sequential,
    // Reduce when all the child frames have terminated.
    FanIn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrameSpecRef {
    Graph(GraphRef),
    Expert
}

impl FrameSpecRef {
    pub fn into_graph_ref(self) -> GraphRef {
        if let Self::Graph(graph_ref) = self {
            graph_ref
        } else {
            panic!("not a graph ref")
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameNode {
    pub session_id: SessionId,
    pub id: FrameId,
    pub status: FrameStatus,
    pub frame_policy: FramePolicy,
    pub superstep: u32,
    pub barrier: bool,
    pub forked_from: Option<SnapshotRef>,
    pub inherited_channels: HashMap<ChannelName, Value>,
    pub spec_ref: FrameSpecRef,
    pub data: FrameData,
    parent: Option<FrameId>,
    next_sibling: Option<FrameId>,
    prev_sibling: Option<FrameId>,
    children: Vec<FrameId>,
}

impl FrameNode {
    pub fn is_waiting_children(&self) -> bool {
        matches!(self.status, FrameStatus::WaitingChildren)
    }

    pub fn has_terminated(&self) -> bool {
        matches!(self.status, FrameStatus::Failed(_) | FrameStatus::Completed)
    }

    pub fn iter_children(&self) -> impl Iterator<Item=FrameId> {
        self.children.iter().cloned()
    }
}

impl FrameNode {
    pub fn new(id: FrameId, args: NewFrameNodeArgs) -> Self 
    {
        Self {
            session_id: args.session_id,
            id,
            data: args.data,
            forked_from: args.forked_from,
            status: FrameStatus::default(),
            inherited_channels: args.inherited_channels,
            superstep: args.superstep,
            frame_policy: args.frame_policy,
            barrier: args.barrier,
            spec_ref: args.spec_ref,
            parent: None,
            next_sibling: None,
            prev_sibling: None,
            children: vec![],
        }
    }
}

pub struct FrameNodeContainer {
    pub store: PgStore,
    pub node: FrameNode,
    pub dirty: bool
}

impl FrameNodeContainer {
    /// Charge le frame `frame_id` de la session `session_id` depuis `store`
    /// — point d'entrée unique de l'arène (voir `FrameTree::arena`) vers
    /// Postgres : un noeud n'existe dans le cache qu'enveloppé dans ce
    /// conteneur, jamais comme `FrameNode` nu, pour que toute mutation passe
    /// par le suivi `dirty`/[`Self::flush`].
    pub async fn new(store: PgStore, session_id: SessionId, frame_id: FrameId) -> Result<Self, sqlx::Error> {
        let node = store.get_frame(&session_id, &frame_id).await?;

        Ok(Self { store, node, dirty: false })
    }

    /// Enveloppe un [`FrameNode`] fraîchement créé en mémoire, pas encore
    /// persisté (voir `FrameTree::create_node`) — `dirty` à `true` d'emblée
    /// pour qu'il soit écrit au premier [`Self::flush`]/à son éviction du
    /// cache, contrairement à [`Self::new`] qui charge un noeud déjà en base.
    fn from_node(store: PgStore, node: FrameNode) -> Self {
        Self { store, node, dirty: true }
    }

    /// Persiste `node` si `dirty` est levé, puis rabaisse le drapeau —
    /// sans effet sinon, pour éviter un aller-retour Postgres à chaque
    /// flush d'un noeud jamais modifié depuis le précédent. Erreur brute
    /// `sqlx::Error`, comme [`Self::new`] : c'est à l'appelant qui la
    /// traverse via un cache (voir [`FrameTree::try_get`]) de l'envelopper
    /// en [`SessionError`].
    pub async fn flush(&mut self) -> Result<(), sqlx::Error> {
        if !self.dirty {
            return Ok(());
        }

        self.store.upsert_frame(self.node.clone()).await?;
        self.dirty = false;

        Ok(())
    }

    /// Supprime le frame de `store` — appelé par [`FrameTree::remove`] une
    /// fois le noeud détaché de l'arbre. Rabaisse aussi `dirty` à `false` :
    /// sans ça, si ce conteneur est encore référencé ailleurs (un appelant
    /// qui l'a obtenu via [`FrameTree::get`] juste avant), son éviction du
    /// cache déclencherait un `upsert_frame` (voir `Drop`) qui ressusciterait
    /// la ligne qu'on vient de supprimer.
    pub async fn delete(&mut self) -> Result<(), sqlx::Error> {
        self.store.delete_frame(&self.node.session_id, &self.node.id).await?;
        self.dirty = false;

        Ok(())
    }
}

impl DerefMut for FrameNodeContainer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        &mut self.node
    }
}

impl Deref for FrameNodeContainer {
    type Target = FrameNode;

    fn deref(&self) -> &Self::Target {
        &self.node
    }
}

impl Drop for FrameNodeContainer {
    /// `Drop` ne peut pas être `async` : à l'éviction du cache (voir
    /// `FrameTree::arena`), on détache un `tokio::spawn` fire-and-forget
    /// plutôt que de bloquer sur [`Self::flush`]. Personne n'attend le
    /// résultat ici, donc une erreur est seulement tracée, pas remontée.
    fn drop(&mut self) {
        if !self.dirty {
            return;
        }

        let store = self.store.clone();
        let node = self.node.clone();

        tokio::spawn(async move {
            if let Err(err) = store.upsert_frame(node).await {
                tracing::warn!("échec de la persistance d'un frame évincé du cache: {err}");
            }
        });
    }
}

pub struct FrameTree {
    pub store: PgStore,
    pub session_id: SessionId,
    pub session: Arc<Mutex<Session>>,
    pub graphs: Graphs,
    pub arena: Cache<FrameId, Arc<Mutex<FrameNodeContainer>>>
}

impl FrameTree {
    pub fn new(store: PgStore, graphs: Graphs, session_id: SessionId, session: Arc<Mutex<Session>>) -> Self {
        Self { store, graphs, session_id, session, arena: Cache::builder().build() }
    }

    pub async fn common_spec_of(&self, frame_id: &FrameId) -> CommonSpec {
        let spec_ref = self.get(frame_id).await.lock().spec_ref.clone();
        match spec_ref {
            FrameSpecRef::Graph(graph_ref) => {
                let graph_spec = self.graphs.get(&graph_ref).await.unwrap();
                graph_spec.common
            },
            FrameSpecRef::Expert => {
                CommonSpec::expert()
            }
        }
    }

    pub async fn spec_ref_of(&self, frame_id: &FrameId) -> FrameSpecRef {
        self.get(frame_id).await.lock().spec_ref.clone()
    }
    
    pub async fn set_root(&self, args: NewFrameNodeArgs) -> FrameId {
        let id = self.create_node(args).await;
        self.session.lock().root_frame = Some(id);
        id
    }

    pub async fn insert(&self, parent: &FrameId, args: NewFrameNodeArgs, position: usize) -> FrameId {
        let id = self.create_node(args).await;
        self.insert_child(parent, &id, position).await;
        id
    }

    pub async fn append(&self, parent: &FrameId, args: NewFrameNodeArgs) -> FrameId {
        let position = match self.arena.get(parent).await {
            Some(container) => container.lock().children.len(),
            None => 0,
        };
        self.insert(parent, args, position).await
    }

    /// Noeud `id`, depuis l'arène si déjà chargée, sinon chargé depuis
    /// `store` (voir [`FrameNodeContainer::new`]) et mis en cache pour les
    /// accès suivants — même idiome que
    /// `session::controller::SessionController::get` sur son propre cache
    /// de [`Session`].
    ///
    /// Panique si le frame n'existe pas ou si le chargement échoue : à
    /// n'utiliser que là où l'appelant sait déjà (par construction de
    /// l'arbre) que `id` est un noeud valide — voir [`Self::try_get`] pour
    /// le chemin qui laisse l'appelant décider.
    pub async fn get(&self, id: &FrameId) -> Arc<Mutex<FrameNodeContainer>> {
        self.try_get(id).await.expect("frame introuvable ou erreur de stockage")
    }

    /// Comme [`Self::get`], sans paniquer : `Err` si le noeud n'est ni en
    /// cache ni en base, ou si la lecture Postgres échoue.
    pub async fn try_get(&self, id: &FrameId) -> Result<Arc<Mutex<FrameNodeContainer>>, SessionError> {
        let store = self.store.clone();
        let session_id = self.session_id;
        let frame_id = *id;

        self.arena
            .try_get_with(*id, async move {
                FrameNodeContainer::new(store, session_id, frame_id)
                    .await
                    .map(|container| Arc::new(Mutex::new(container)))
            })
            .await
            .map_err(SessionError::StorageError)
    }

    /// Clone du statut plutôt qu'une référence : `id` peut être chargé (ou
    /// évincé puis rechargé) au fil de l'appel, via [`Self::get`], donc rien
    /// ne garantit la durée de vie d'un emprunt sur le noeud en cache.
    pub async fn status_of(&self, id: &FrameId) -> FrameStatus {
        self.get(id).await.lock().status.clone()
    }

    pub async fn data_of(&self, id: &FrameId) -> FrameData {
        self.get(id).await.lock().data.clone()
    }

    pub async fn policy_of(&self, id: &FrameId) -> FramePolicy {
        self.get(id).await.lock().frame_policy.clone()
    }

    pub async fn set_status(&self, id: &FrameId, status: FrameStatus) {
        self.get(id).await.lock().status = status;
    }

    pub async fn parent_of(&self, id: &FrameId) -> Option<FrameId> {
        self.arena.get(id).await.and_then(|container| container.lock().parent)
    }

    pub async fn next_sibling_of(&self, id: &FrameId) -> Option<FrameId> {
        self.arena.get(id).await.and_then(|container| container.lock().next_sibling)
    }

    pub async fn prev_sibling_of(&self, id: &FrameId) -> Option<FrameId> {
        self.arena.get(id).await.and_then(|container| container.lock().prev_sibling)
    }

    /// Collecté en un `Vec` plutôt qu'un itérateur paresseux sur `&FrameTree`
    /// (comme l'ancienne version adossée à un `HashMap`) : l'arène est un
    /// cache asynchrone (voir [`Self::arena`]), donc chaque accès à un noeud
    /// — y compris `parent` pour ses enfants — passe par un `.await`, ce
    /// qu'un `Iterator::next` synchrone ne peut pas exprimer.
    pub async fn iter_children_of(&self, parent: &FrameId) -> impl Iterator<Item=FrameId> {
        match self.arena.get(parent).await {
            Some(container) => container.lock().children.clone().into_iter(),
            None => Vec::new().into_iter(),
        }
    }

    pub async fn last_child_of(&self, parent: &FrameId) -> Option<FrameId> {
        self.get(parent).await.lock().children.last().copied()
    }

    /// Comme [`Self::iter_children_of`], collecté pour la même raison : le
    /// saut d'un sibling au suivant est lui-même un accès à l'arène.
    pub async fn iter_next_siblings(&self, id: &FrameId) -> Vec<FrameId> {
        let mut siblings = Vec::new();
        let mut current = self.next_sibling_of(id).await;

        while let Some(curr) = current {
            current = self.next_sibling_of(&curr).await;
            siblings.push(curr);
        }

        siblings
    }

    pub async fn iter_prev_siblings(&self, id: &FrameId) -> Vec<FrameId> {
        let mut siblings = Vec::new();
        let mut current = self.prev_sibling_of(id).await;

        while let Some(curr) = current {
            current = self.prev_sibling_of(&curr).await;
            siblings.push(curr);
        }

        siblings
    }

    /// Détache puis retire `id` et tout son sous-arbre de l'arène, en
    /// supprimant chaque noeud de `store` au passage (voir
    /// [`FrameNodeContainer::delete`]) — pile explicite plutôt que
    /// récursion (comme les autres traversées de ce module) : une
    /// `async fn` ne peut pas s'appeler récursivement sans `Box::pin`
    /// (taille de future infinie sinon), et chaque noeud nécessite de toute
    /// façon un aller-retour au cache pour lire ses enfants.
    ///
    /// S'arrête à la première erreur de suppression, en laissant le sous-arbre
    /// pas encore traité (dont `id` lui-même s'il échoue immédiatement)
    /// intact dans l'arène et en base — l'appelant peut réessayer sur le
    /// même `id`.
    pub async fn remove(&mut self, id: &FrameId) -> Result<(), sqlx::Error> {
        let mut stack = vec![*id];

        while let Some(current) = stack.pop() {
            self.detach(&current).await;

            if let Some(container) = self.arena.get(&current).await {
                stack.extend(container.lock().children.clone());
                container.lock().delete().await?;
            }

            self.arena.remove(&current).await;
        }

        Ok(())
    }
}

impl FrameTree {
    async fn create_node(&self, args: NewFrameNodeArgs) -> FrameId {
        let id = FrameId::new();
        let node = FrameNode::new(id, args);
        let container = FrameNodeContainer::from_node(self.store.clone(), node);
        self.arena.insert(id, Arc::new(Mutex::new(container))).await;
        id
    }

    async fn insert_child(&self, parent: &FrameId, child: &FrameId, index: usize) {
        self.detach(child).await;

        let Some(parent_container) = self.arena.get(parent).await else {
            return;
        };

        let (index, prev, next) = {
            let parent_node = parent_container.lock();
            let index = index.min(parent_node.children.len());
            let prev = index.checked_sub(1).and_then(|i| parent_node.children.get(i).copied());
            let next = parent_node.children.get(index).copied();
            (index, prev, next)
        };

        if let Some(prev) = prev {
            self.link_siblings(&prev, child).await;
        } else if let Some(child_container) = self.arena.get(child).await {
            child_container.lock().prev_sibling = None;
        }

        if let Some(next) = next {
            self.link_siblings(child, &next).await;
        } else if let Some(child_container) = self.arena.get(child).await {
            child_container.lock().next_sibling = None;
        }

        parent_container.lock().children.insert(index, *child);

        if let Some(child_container) = self.arena.get(child).await {
            child_container.lock().parent = Some(*parent);
        }
    }

    /// Détache le noeud de son parent et de ses siblings, l'isolant du reste de l'arbre
    /// sans le retirer de l'arène. Si le noeud était la racine, celle-ci est réinitialisée.
    async fn detach(&self, id: &FrameId) {
        self.detach_from_siblings(id).await;
        self.detach_from_parent(id).await;

        let mut session = self.session.lock();
        if session.root_frame == Some(*id) {
            session.root_frame = None;
        }
    }

    async fn detach_from_parent(&self, id: &FrameId) {
        let Some(parent_id) = self.arena.get(id).await.and_then(|container| container.lock().parent) else {
            return;
        };

        if let Some(parent_container) = self.arena.get(&parent_id).await {
            parent_container.lock().children.retain(|child| child != id);
        }

        if let Some(container) = self.arena.get(id).await {
            container.lock().parent = None;
        }
    }

    async fn detach_from_siblings(&self, id: &FrameId) {
        let Some(container) = self.arena.get(id).await else {
            return;
        };

        let (prev, next) = {
            let node = container.lock();
            (node.prev_sibling, node.next_sibling)
        };

        if let Some(prev_id) = prev {
            if let Some(prev_container) = self.arena.get(&prev_id).await {
                prev_container.lock().next_sibling = next;
            }
        }

        if let Some(next_id) = next {
            if let Some(next_container) = self.arena.get(&next_id).await {
                next_container.lock().prev_sibling = prev;
            }
        }

        let mut node = container.lock();
        node.prev_sibling = None;
        node.next_sibling = None;
    }

    async fn link_siblings(&self, prev: &FrameId, next: &FrameId) {
        if let Some(prev_container) = self.arena.get(prev).await {
            prev_container.lock().next_sibling = Some(*next);
        }

        if let Some(next_container) = self.arena.get(next).await {
            next_container.lock().prev_sibling = Some(*prev);
        }
    }
}
