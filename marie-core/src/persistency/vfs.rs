use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt as _, path::Path as ObjectPath};
use sqlx::postgres::PgPool;

use crate::{
    persistency::{
        alias::PostgresAliasCatalog,
        filesystem::VFS,
        inode::{InodeCatalog as _, ObjectFileSystem, PostgresInodeCatalog},
        var::{VarFileSystem, VarStore, WorkspaceVarStore},
    },
    session::SessionId,
    workspace::{WorkspaceId, client::WorkspaceClient},
};

/// Compose le VFS d'un workspace (`/var`, `/files`) et, par-dessus, celui
/// d'une session (`/session/var`, `/session/files`) — voir la doc de [`VFS`]
/// pour le principe de composition (le VFS d'une session est un `VFS` monté
/// à l'intérieur de celui de son workspace, pas un type séparé).
///
/// Ne connaît volontairement pas `session::client::SessionClient` (qui, à
/// l'inverse, détient un `WorkspaceVfs` pour construire le VFS de ses
/// sessions, voir [`SessionClient::vfs`](crate::session::client::SessionClient::vfs)) :
/// un tel couplage serait circulaire. Le store `/var` d'une session (voir
/// [`Self::session_vfs`]) est donc fourni par l'appelant plutôt que construit
/// ici.
#[derive(Clone)]
pub struct WorkspaceVfs {
    workspace: WorkspaceClient,
    pool: PgPool,
    store: Arc<dyn ObjectStore>,
}

impl WorkspaceVfs {
    pub fn new(workspace: WorkspaceClient, pool: PgPool, store: Arc<dyn ObjectStore>) -> Self {
        Self { workspace, pool, store }
    }

    /// VFS d'un workspace seul (`/var`, `/files`) — sans `/session`.
    pub async fn vfs(&self, workspace_id: WorkspaceId) -> anyhow::Result<Arc<VFS>> {
        let aliases = Arc::new(PostgresAliasCatalog::for_workspace(self.pool.clone(), workspace_id));
        let vfs = Arc::new(VFS::with_aliases(aliases));

        let var = VarFileSystem::new(Arc::new(WorkspaceVarStore::new(self.workspace.clone(), workspace_id)));
        vfs.mount("/var", Arc::new(var)).await;

        let catalog = PostgresInodeCatalog::for_workspace(self.pool.clone(), workspace_id).await?;
        vfs.mount("/files", Arc::new(ObjectFileSystem::new(Arc::new(catalog), self.store.clone()))).await;

        Ok(vfs)
    }

    /// VFS d'une session seule (`/var`, `/files`) — `workspace_id` n'est
    /// jamais optionnel : une session n'existe que rattachée à un workspace
    /// dès sa création (voir `workspace::client::WorkspaceClient::create_session`),
    /// le catalogue d'inodes de son `/files` est structurellement un
    /// sous-arbre de celui de ce workspace (voir
    /// [`PostgresInodeCatalog::for_session`]). `var` est fourni par
    /// l'appelant (voir la doc de [`Self`]).
    pub async fn session_vfs(&self, workspace_id: WorkspaceId, session_id: SessionId, var: Arc<dyn VarStore>) -> anyhow::Result<Arc<VFS>> {
        let aliases = Arc::new(PostgresAliasCatalog::for_session(self.pool.clone(), session_id));
        let vfs = Arc::new(VFS::with_aliases(aliases));
        vfs.mount("/var", Arc::new(VarFileSystem::new(var))).await;

        let catalog = PostgresInodeCatalog::for_session(self.pool.clone(), workspace_id, session_id).await?;
        vfs.mount("/files", Arc::new(ObjectFileSystem::new(Arc::new(catalog), self.store.clone()))).await;

        Ok(vfs)
    }

    /// VFS complet d'une session : celui de son workspace (voir
    /// [`Self::vfs`]), avec `/session` monté par-dessus (voir
    /// [`Self::session_vfs`]).
    pub async fn mount_session(&self, workspace_id: WorkspaceId, session_id: SessionId, var: Arc<dyn VarStore>) -> anyhow::Result<Arc<VFS>> {
        let vfs = self.vfs(workspace_id).await?;
        let session_vfs = self.session_vfs(workspace_id, session_id, var).await?;
        vfs.mount("/session", session_vfs).await;
        Ok(vfs)
    }

    /// Supprime récursivement `/files` d'une session (voir
    /// [`PostgresInodeCatalog::for_session`] puis [`InodeCatalog::remove`])
    /// et les objets associés dans l'`ObjectStore` — utilisé par
    /// `RpcCall::DELETE_SESSION` (`network::persistency`), qui n'a pas de VFS
    /// de session déjà monté pour passer par `FileSystem::remove`. Sans
    /// effet si la session n'a pas de fichiers (jamais écrit, ou déjà purgée).
    pub async fn delete_session_files(&self, workspace_id: WorkspaceId, session_id: SessionId) -> anyhow::Result<()> {
        let catalog = PostgresInodeCatalog::for_session(self.pool.clone(), workspace_id, session_id).await?;
        let object_keys = catalog.remove("/").await?;
        for object_key in object_keys {
            self.store.delete(&ObjectPath::from(object_key)).await?;
        }
        Ok(())
    }
}
