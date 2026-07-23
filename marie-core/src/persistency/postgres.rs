use anyhow::Context as _;
use sqlx::postgres::{PgPool, PgPoolOptions};

/// Applique les migrations SQL versionnées embarquées à la compilation
/// depuis `marie-core/migrations/` (voir [`sqlx::migrate!`]) : toutes les
/// tables à schéma fixe de ce module — `fs_alias` (voir
/// [`super::alias::AliasCatalog`]), `fs_inode` (voir
/// [`super::inode::InodeCatalog`]), et une table par objet du domaine
/// persisté via un trait CRUD spécifique (`session`, `workspace`, `model`,
/// `tool`, `expert` — voir `persistency::SessionStore`/`WorkspaceStore` et
/// `model`/`tools`/`expert::catalog::store`) — suivies dans une table
/// `_sqlx_migrations` gérée par `sqlx` : idempotent, sûr à rappeler à chaque
/// démarrage d'un nœud plutôt qu'une fois pour tout le cluster.
///
/// À appeler une fois par l'appelant après avoir ouvert son [`PgPool`],
/// avant de construire tout composant `Postgres*` de ce module.
pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::migrate!().run(pool).await?;
    Ok(())
}

/// Poignée PostgreSQL partagée par chaque trait CRUD spécifique du cluster
/// (voir [`run_migrations`] pour la liste des tables qu'ils utilisent) —
/// alternative centralisée/administrée à [`super::store::RedbStore`] pour les
/// déploiements qui préfèrent un stockage partagé (ex: conteneurs sans disque
/// local durable, plusieurs process partageant le même catalogue) plutôt
/// qu'un fichier `redb` embarqué par nœud.
///
/// Comme [`RedbStore`](super::store::RedbStore), volontairement pas de trait
/// CRUD générique par-dessus ce type : chaque objet du domaine a son propre
/// trait, implémenté directement pour [`PostgresStore`] là où il est défini,
/// contre sa propre table dédiée (schéma fixe, migré — voir
/// [`run_migrations`] — plutôt que créé dynamiquement à l'exécution).
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    /// Ouvre un pool de connexions vers `database_url`.
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new().connect(database_url).await.context("connexion au pool PostgreSQL")?;
        Ok(Self::from_pool(pool))
    }

    /// Comme [`Self::connect`], à partir d'un [`PgPool`] déjà configuré par
    /// l'appelant (taille de pool, timeouts, etc.).
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub(crate) fn pool(&self) -> &PgPool {
        &self.pool
    }
}
