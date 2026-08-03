use std::ops::{Deref, DerefMut};

use crate::session::SessionId;

use super::store::SessionFrameStore;
use super::{FrameId, FrameNode};

pub struct FrameNodeContainer {
    pub store: SessionFrameStore,
    pub node: FrameNode,
    pub dirty: bool
}

impl FrameNodeContainer {
    /// Charge le frame `frame_id` de la session `session_id` depuis `store`
    /// — point d'entrée unique de l'arène (voir `FrameTree::arena`) vers
    /// [`StoreSession`] : un noeud n'existe dans le cache qu'enveloppé dans
    /// ce conteneur, jamais comme `FrameNode` nu, pour que toute mutation
    /// passe par le suivi `dirty`/[`Self::flush`].
    pub async fn new(store: SessionFrameStore, session_id: SessionId, frame_id: FrameId) -> crate::Result<Self> {
        let node = store.get_frame(&session_id, &frame_id).await?;

        Ok(Self { store, node, dirty: false })
    }

    /// Enveloppe un [`FrameNode`] fraîchement créé en mémoire, pas encore
    /// persisté (voir `FrameTree::create_node`) — `dirty` à `true` d'emblée
    /// pour qu'il soit écrit au premier [`Self::flush`]/à son éviction du
    /// cache, contrairement à [`Self::new`] qui charge un noeud déjà en base.
    pub(super) fn from_new_node(store: SessionFrameStore, node: FrameNode) -> Self {
        Self { store, node, dirty: true }
    }

    /// Persiste `node` si `dirty` est levé, puis rabaisse le drapeau —
    /// sans effet sinon, pour éviter un aller-retour au store à chaque
    /// flush d'un noeud jamais modifié depuis le précédent. Erreur brute
    /// `crate::Error`, comme [`Self::new`] : c'est à l'appelant qui la
    /// traverse via un cache (voir [`FrameTree::try_get`]) de l'envelopper
    /// en [`SessionError`].
    pub async fn flush(&mut self) -> crate::Result<()> {
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
    pub async fn delete(&mut self) -> crate::Result<()> {
        self.store.delete_frame(&self.node.session_id, &self.node.id).await?;
        self.dirty = false;

        Ok(())
    }
}

impl Deref for FrameNodeContainer {
    type Target = FrameNode;

    fn deref(&self) -> &Self::Target {
        &self.node
    }
}

impl DerefMut for FrameNodeContainer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        &mut self.node
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
