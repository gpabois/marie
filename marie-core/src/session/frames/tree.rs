use std::sync::Arc;

use moka::future::Cache;
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::Value;
use typed_builder::TypedBuilder;

use crate::{di::{Constructible, Factory, Get, Resolve}, graph::Graphs, hitl::HitlId, session::{SessionId, controller::SessionError, frames::{FrameId, ResumePolicy, policy::{OnResumePolicy, ReducePolicy}, store::SessionFrameStore}, spec::CommonSpec}};

use crate::session::run_log::RunLog;

use super::{
    NewFrameNodeArgs, FrameNode,
    FrameNodeContainer, 
    FrameSpecRef, 
    FrameStatus, 
    FrameKind, 
    FramePolicy
};

pub type FrameTreeFactory = Factory<FrameTree, SessionId>;

impl<C> Constructible<C> for FrameTreeFactory 
    where C: Get<SessionFrameStore> + Resolve<Graphs> + Clone + Send + Sync + 'static

{
    fn construct(container: &C, _: ()) -> Self {
        let container = container.clone();

        Self::new(move |session_id| {
            let args = FrameTreeArgs::builder()
                .store(container.get())
                .graphs(container.resolve(()))
                .session_id(session_id)
                .build();

            FrameTree::new(args)
        })
    }
}

#[derive(TypedBuilder)]
pub struct FrameTreeArgs {
    store: SessionFrameStore,
    session_id: SessionId,
    graphs: Graphs
}

#[derive(Clone)]
pub struct FrameTree {
    pub store: SessionFrameStore,
    pub session_id: SessionId,
    pub graphs: Graphs,
    pub arena: Cache<FrameId, Arc<Mutex<FrameNodeContainer>>>
}

impl FrameTree {
    pub fn new(args: FrameTreeArgs) -> Self {
        Self { 
            store: args.store, 
            graphs: args.graphs, 
            session_id: args.session_id, 
            arena: Cache::builder().build() 
        }
    }

    pub async fn frame_id_bound_to_hitl_id(&self, id: &HitlId) -> Result<Option<FrameId>, SessionError> {
        self.store
            .get_frame_id_by_hitl_id(&self.session_id, *id)
            .await
            .map_err(|err| SessionError::StorageError(Arc::new(err)))
    }

    pub async fn common_spec_of(&self, frame_id: &FrameId) -> CommonSpec {
        let spec_ref = self.get(frame_id).await.lock().spec_ref.clone();
        match spec_ref {
            FrameSpecRef::Graph(graph_ref) => {
                let graph_spec = self.graphs.get(&graph_ref).await.unwrap();
                graph_spec.common
            },
            FrameSpecRef::Hitl => {
                CommonSpec::hitl()
            },
            FrameSpecRef::ToolCall => {
                CommonSpec::tool()
            },
            FrameSpecRef::ToolAggregator => {
                CommonSpec::tool_aggregator()
            },
            FrameSpecRef::Expert => {
                CommonSpec::expert()
            },
            FrameSpecRef::ExpertAggregator => {
                CommonSpec::expert_aggregator()
            }
        }
    }

    pub async fn spec_ref_of(&self, frame_id: &FrameId) -> FrameSpecRef {
        self.get(frame_id).await.lock().spec_ref.clone()
    }
    
    /// Marque le noeud créé comme racine de l'arbre (voir
    /// [`FrameNode::is_root`]) — simple positionnement du drapeau sur le
    /// noeud tout juste créé (pas encore persisté, voir
    /// [`FrameNodeContainer::flush`]), pas un appel à
    /// `StoreSessionFrame::set_root_frame_id` : celui-ci exige une ligne déjà
    /// en base, ce qui n'a pas de sens ici puisqu'aucune autre frame de la
    /// session n'existe encore à rabaisser.
    pub async fn set_root(&self, args: NewFrameNodeArgs) -> FrameId {
        let id = self.create_node(args).await;
        if let Some(container) = self.arena.get(&id).await {
            container.lock().is_root = true;
        }
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

    pub async fn data_of(&self, id: &FrameId) -> FrameKind {
        self.get(id).await.lock().data.clone()
    }

    pub async fn policy_of(&self, id: &FrameId) -> FramePolicy {
        self.get(id).await.lock().frame_policy.clone()
    }

    pub async fn set_policy_of(&self, id: &FrameId, policy: FramePolicy) {
        self.get(id).await.lock().frame_policy = policy;
    } 

    pub async fn set_resume_policy(&self, id: &FrameId, resume_policy: ResumePolicy) {
        let mut policy = self.policy_of(id).await;
        policy.resume_policy = resume_policy;
        self.set_policy_of(id, policy);
    }


    pub async fn set_on_resume_policy(&self, id: &FrameId, on_resume_policy: OnResumePolicy) {
        let mut policy = self.policy_of(id).await;
        policy.on_resume_policy = on_resume_policy;
        self.set_policy_of(id, policy);       
    }

    pub async fn on_resume_policy(&self, id: &FrameId) -> OnResumePolicy {
        let policy = self.policy_of(id).await;
        policy.on_resume_policy.clone()
    }

    pub async fn set_reduce_policy(&self, id: &FrameId, reduce_policy: ReducePolicy) {
        let mut policy = self.policy_of(id).await;
        policy.reduce_policy = reduce_policy;
        self.set_policy_of(id, policy);       
    }

    pub async fn logs_of(&self, id: &FrameId) -> Vec<RunLog> {
        self.get(id).await.lock().logs.clone()
    }

    /// La dernière entrée non résolue de `id` (voir la doc de
    /// [`crate::session::run_log::RunLogs`] : au plus une à la fois par
    /// construction) — `None` si aucune réservation n'est en attente.
    pub async fn pending_log_of(&self, id: &FrameId) -> Option<RunLog> {
        self.get(id).await.lock().logs.iter().rev().find(|l| l.value.is_none()).cloned()
    }

    /// Ajoute les entrées nouvellement réservées par un run (voir
    /// `FrameResponse::new_logs`) — no-op si `new_logs` est vide, pour éviter
    /// de marquer `dirty` un frame dont ce run n'a rien réservé de nouveau.
    pub async fn append_logs(&self, id: &FrameId, mut new_logs: Vec<RunLog>) {
        if new_logs.is_empty() {
            return;
        }
        self.get(id).await.lock().logs.append(&mut new_logs);
    }

    /// Fixe la valeur résolue de l'entrée `index` de `id` — appelée par
    /// `SessionHandler::resolve_pending_log` une fois qu'un enfant pertinent
    /// a fourni la réponse attendue.
    pub async fn resolve_log(&self, id: &FrameId, index: u32, value: impl Serialize) {
        let container = self.get(id).await;
        let mut guard = container.lock();
        if let Some(entry) = guard.logs.iter_mut().find(|l| l.index == index) {
            entry.value = Some(serde_json::to_value(value).unwrap());
        }
    }

    pub async fn set_status(&self, id: &FrameId, status: FrameStatus) {
        self.get(id).await.lock().status = status;
    }

    /// Lie l'entrée `index` du journal de rejeu de `id` à `hitl_id` — posé
    /// une fois la requête envoyée à
    /// [`crate::hitl::service::SessionHitls::request`] (voir
    /// [`FrameNode::bind_hitl_to_log`] pour le détail), pour que
    /// [`Self::log_index_bound_to_hitl_id`] retrouve directement l'entrée à
    /// résoudre une fois la réponse arrivée.
    pub async fn bind_hitl_to_log(&self, id: &FrameId, index: u32, hitl_id: HitlId) {
        self.get(id).await.lock().bind_hitl_to_log(index, hitl_id);
    }

    /// L'[`HitlId`] lié à l'entrée `index` du journal de rejeu de `id`,
    /// s'il y en a un — voir [`Self::bind_hitl_to_log`].
    pub async fn hitl_id_bound_to_log(&self, id: &FrameId, index: u32) -> Option<HitlId> {
        self.get(id).await.lock().hitl_id_of_log(index)
    }

    /// L'index du journal de rejeu de `id` lié à `hitl_id`, s'il y en a un
    /// — sens inverse de [`Self::hitl_id_bound_to_log`].
    pub async fn log_index_bound_to_hitl_id(&self, id: &FrameId, hitl_id: HitlId) -> Option<u32> {
        self.get(id).await.lock().log_index_of_hitl(hitl_id)
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
    pub async fn remove(&mut self, id: &FrameId) -> crate::Result<()> {
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
        let container = FrameNodeContainer::from_new_node(self.store.clone(), node);
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

    /// Détache le noeud de son parent et de ses siblings, l'isolant du reste
    /// de l'arbre sans le retirer de l'arène. `is_root` (voir
    /// [`FrameNode::is_root`]) n'a pas besoin d'être touché ici : une racine
    /// n'a par construction pas de parent, donc n'est jamais la cible d'un
    /// détachement en vue d'un rattachement ailleurs — seule sa suppression
    /// (voir [`Self::remove`]) y met fin, en retirant la ligne elle-même.
    async fn detach(&self, id: &FrameId) {
        self.detach_from_siblings(id).await;
        self.detach_from_parent(id).await;
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
