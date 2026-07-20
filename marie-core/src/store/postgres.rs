use anyhow::Context as _;
use sqlx::postgres::{PgPool, PgPoolOptions};

/// Applique les migrations SQL versionnées embarquées à la compilation
/// depuis `marie-core/migrations/` (voir [`sqlx::migrate!`]) : une table par
/// objet du domaine persisté via un trait CRUD spécifique (`session` — voir
/// `session::store::SessionStore` — pour l'instant), suivies dans une table
/// `_sqlx_migrations` gérée par `sqlx` : idempotent, sûr à rappeler à chaque
/// démarrage d'un nœud plutôt qu'une fois pour tout le cluster.
///
/// À appeler une fois par l'appelant après avoir ouvert son [`PgPool`], avant
/// de construire tout composant `PgStore`.
pub async fn run_migrations(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::migrate!().run(pool).await.context("application des migrations PostgreSQL")?;
    Ok(())
}

/// Poignée PostgreSQL partagée par chaque trait CRUD spécifique du cluster
/// (voir [`run_migrations`] pour la liste des tables qu'ils utilisent) —
/// alternative centralisée/administrée pour les déploiements qui préfèrent un
/// stockage partagé (ex: conteneurs sans disque local durable, plusieurs
/// process partageant le même catalogue) plutôt qu'un fichier embarqué par
/// nœud.
///
/// Volontairement pas de trait CRUD générique par-dessus ce type : chaque
/// objet du domaine a son propre trait, implémenté directement pour
/// [`PgStore`] là où il est défini, contre sa propre table dédiée (schéma
/// fixe, migré — voir [`run_migrations`] — plutôt que créé dynamiquement à
/// l'exécution).
#[derive(Clone)]
pub struct PgStore(PgPool);

impl PgStore {
    /// Ouvre un pool de connexions vers `database_url`.
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new().connect(database_url).await.context("connexion au pool PostgreSQL")?;
        Ok(Self::from_pool(pool))
    }

    /// Comme [`Self::connect`], à partir d'un [`PgPool`] déjà configuré par
    /// l'appelant (taille de pool, timeouts, etc.).
    pub fn from_pool(pool: PgPool) -> Self {
        Self(pool)
    }

    pub(crate) fn pool(&self) -> &PgPool {
        &self.0
    }
}
