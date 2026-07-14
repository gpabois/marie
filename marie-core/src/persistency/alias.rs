use async_trait::async_trait;
use sqlx::{Row as _, postgres::PgPool};

use crate::{session::SessionId, workspace::WorkspaceId};

/// Table d'alias d'un [`crate::persistency::filesystem::VFS`] : une entrée
/// `from -> to` se comporte comme un lien symbolique Unix sur un dossier
/// (voir la doc de `VFS` pour la sémantique de résolution). Scopée comme le
/// reste du VFS (voir `persistency::inode::PostgresInodeCatalog`) : une
/// instance par workspace, une par session, jamais partagées entre elles.
#[async_trait]
pub trait AliasCatalog: Send + Sync {
    /// Alias dont la clé (`from_path`) est le plus long préfixe de `path` —
    /// même logique que le routage de montage dans `VFS::resolve_mount`.
    /// Renvoie `path` avec ce préfixe substitué par sa cible, ou `None` si
    /// aucun alias ne préfixe `path`.
    async fn resolve_prefix(&self, path: &str) -> anyhow::Result<Option<String>>;

    /// Définit (crée ou remplace) l'alias `from -> to`.
    async fn set(&self, from: &str, to: &str) -> anyhow::Result<()>;

    /// Retire l'alias `from` — sans effet s'il n'existe pas.
    async fn remove(&self, from: &str) -> anyhow::Result<()>;

    /// Tous les alias de ce scope, `(from, to)`.
    async fn list(&self) -> anyhow::Result<Vec<(String, String)>>;
}

/// [`AliasCatalog`] adossé à PostgreSQL — une ligne par alias plutôt qu'un
/// blob par scope, pour éviter une course lecture-modification-écriture si
/// deux nœuds posent des alias différents au même moment sur le même scope
/// (voir la table `fs_alias`, dont le schéma est géré par les migrations —
/// voir `persistency::postgres::run_migrations`, à appeler une fois par
/// l'appelant avant de construire ce catalogue).
pub struct PostgresAliasCatalog {
    pool: PgPool,
    scope: String,
}

impl PostgresAliasCatalog {
    /// Alias visibles par tout le VFS d'un workspace (voir
    /// `persistency::vfs::WorkspaceVfs::vfs`).
    pub fn for_workspace(pool: PgPool, workspace_id: WorkspaceId) -> Self {
        Self { pool, scope: format!("workspace:{workspace_id}") }
    }

    /// Alias propres à une session, distincts de ceux de son workspace (voir
    /// `persistency::vfs::WorkspaceVfs::mount_session` : le VFS de session
    /// porte sa propre table d'alias, résolue avant de retomber sur celle du
    /// workspace englobant).
    pub fn for_session(pool: PgPool, session_id: SessionId) -> Self {
        Self { pool, scope: format!("session:{session_id}") }
    }
}

#[async_trait]
impl AliasCatalog for PostgresAliasCatalog {
    /// Charge tous les alias du scope (faible cardinalité attendue, comme le
    /// nombre de montages d'un `VFS`) et fait le plus-long-préfixe côté Rust
    /// plutôt qu'en SQL — même approche que `VFS::resolve_mount`.
    async fn resolve_prefix(&self, path: &str) -> anyhow::Result<Option<String>> {
        let aliases = self.list().await?;

        let best = aliases
            .into_iter()
            .filter(|(from, _)| path == from.as_str() || path.starts_with(&format!("{from}/")))
            .max_by_key(|(from, _)| from.len());

        Ok(best.map(|(from, to)| format!("{to}{}", &path[from.len()..])))
    }

    async fn set(&self, from: &str, to: &str) -> anyhow::Result<()> {
        sqlx::query("INSERT INTO fs_alias (scope, from_path, to_path) VALUES ($1, $2, $3) ON CONFLICT (scope, from_path) DO UPDATE SET to_path = EXCLUDED.to_path")
            .bind(&self.scope)
            .bind(from)
            .bind(to)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn remove(&self, from: &str) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM fs_alias WHERE scope = $1 AND from_path = $2")
            .bind(&self.scope)
            .bind(from)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list(&self) -> anyhow::Result<Vec<(String, String)>> {
        let rows = sqlx::query("SELECT from_path, to_path FROM fs_alias WHERE scope = $1").bind(&self.scope).fetch_all(&self.pool).await?;
        Ok(rows.iter().map(|row| (row.get("from_path"), row.get("to_path"))).collect())
    }
}
