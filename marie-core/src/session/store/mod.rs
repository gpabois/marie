use std::{collections::HashMap, ops::Deref, sync::Arc};

use async_trait::async_trait;
use parking_lot::Mutex;
#[cfg(feature = "catalog")]
use sqlx::Row as _;
#[cfg(feature = "catalog")]
use sqlx::postgres::PgRow;
#[cfg(feature = "catalog")]
use sqlx::types::Json;

use crate::entity::UnitOfWorkExecutor;
use crate::session::dto::SessionView;
use crate::hitl::store::{InMemoryHitlStore, StoreHitl};
use crate::session::frames::store::StoreSessionFrame;
use crate::session::logs::store::{InMemorySessionLogsStore, StoreSessionLogs};
use crate::session::snapshot::store::{InMemorySessionSnapshotStore, StoreSessionSnapshot};
use crate::session::{Session, SessionId, SessionStatus};
#[cfg(feature = "catalog")]
use crate::store::PgStore;

#[async_trait]
pub trait StoreSession: StoreSessionFrame + StoreHitl + StoreSessionLogs + StoreSessionSnapshot {
    async fn get_session(&self, id: &SessionId) -> crate::Result<Session>;
    /// Comme [`Self::get_session`], mais sans décoder la ligne complète —
    /// pour un simple contrôle d'existence (ex. avant d'aller chercher
    /// journal/hitls d'un `session_id` fourni par un appelant externe) qui
    /// n'a pas besoin du [`Session`] déserialisé.
    async fn session_exists(&self, id: &SessionId) -> crate::Result<bool>;
    async fn upsert_session(&self, session: Session) -> crate::Result<()>;
    async fn delete_session(&self, id: &SessionId) -> crate::Result<Session>;

    /// Toutes les sessions connues, sans filtre ni pagination — utilisé par
    /// `SessionController::list_sessions` (voir
    /// [`crate::session::controller::SessionController`]), un consommateur
    /// de gestion (ex. `list sessions` du CLI) qui a besoin de la liste
    /// complète, pas d'une vue agrégée par session comme
    /// [`Self::get_session_view`].
    async fn list_sessions(&self) -> crate::Result<Vec<Session>>;

    /// Vue agrégée d'une session — assemble [`Self::get_session`] (existence
    /// + métadonnées), [`Self::list_log`] (journal complet) et
    /// [`Self::list_pending_hitls`] (hitls en attente) en un seul appel, pour
    /// les appelants qui veulent l'état complet d'une session sans enchaîner
    /// eux-mêmes ces trois requêtes (voir
    /// [`crate::session::client::SessionClient::get_session`]). Comme
    /// [`Self::get_session`], échoue si `session_id` n'existe pas — pas de
    /// vue partielle construite à partir d'une session inconnue.
    async fn get_session_view(&self, session_id: SessionId) -> crate::Result<SessionView>;
}

/// Type opaque enveloppant l'implémentation concrète de [`StoreSession`]
/// utilisée par [`crate::session::controller::SessionController`] et tout ce
/// qui en dérive (`SessionHandler`, `FrameTree`, `Snapshots`, `Hitls`) — ces
/// derniers ne connaissent que `SessionStore`, jamais [`PgStore`] ni
/// [`InMemorySessionStore`] directement, pour pouvoir faire varier le
/// backend (Postgres en production, mémoire pour les tests) sans toucher au
/// reste du module `session`. `Arc<dyn StoreSession + Send + Sync + 'static>`
/// plutôt qu'un paramètre générique : `SessionController`/`SessionHandler`
/// sont déjà des poignées `Clone` bon marché partagées entre de nombreuses
/// tâches tokio (voir leur doc respective), un générique se propagerait à
/// travers tous ces types là où une seule indirection dynamique suffit.
#[derive(Clone)]
pub struct SessionStore(Arc<dyn StoreSession + Send + Sync + 'static>);

impl SessionStore {
    pub fn new(store: impl StoreSession + Send + Sync + 'static) -> Self {
        Self(Arc::new(store))
    }

    pub fn in_memory() -> Self {
        Self::new(InMemorySessionStore::new())
    }
}

#[async_trait]
impl UnitOfWorkExecutor<Session> for SessionStore {
    async fn insert(&self, entity: &Session) -> crate::Result<()> {
        self.upsert_session(entity.clone()).await
    }
    async fn replace(&self, entity: &Session) -> crate::Result<()> {
        self.upsert_session(entity.clone()).await
    }
    async fn delete(&self, entity: &Session) -> crate::Result<()> {
        self.delete_session(&entity.id).await;
        Ok(())
    }
}

impl Deref for SessionStore {
    type Target = dyn StoreSession + Send + Sync + 'static;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

/// Implémentation PostgreSQL de [`StoreSession`], contre les tables
/// `marie_sessions` (voir `migrations/0002_session.sql`,
/// `migrations/0017_session_root_frame.sql` et
/// `migrations/0018_session_status.sql`) — même poignée [`PgStore`] que
/// `catalog`/`workspace` (voir leurs `store.rs` respectifs). L'arbre des
/// frames (`marie_session_frames`) et les hitls (`marie_session_hitls`) sont
/// couverts séparément par `impl `[`StoreSessionFrame`]` for `[`PgStore`]
/// (voir `crate::session::frames::store`) et `impl `[`StoreHitl`]` for
/// `[`PgStore`] (voir `crate::hitl::store`), `StoreSession` en hérite via
/// ses supertraits.
///
/// `marie_sessions` ne porte plus que `root_frame` (voir
/// [`Session::root_frame`]), pas l'arbre entier : depuis
/// `migrations/0015_session_frame.sql`, chaque frame vit dans sa propre
/// ligne de `marie_session_frames`.
/// `status` (voir [`Session::status`]/[`SessionStatus`]) est un statut de
/// session à part entière, sans rapport avec le statut d'un frame individuel
/// (`session::frames::FrameStatus`, porté par `marie_session_frames.node`) —
/// stocké en JSONB comme `node`/`data` des deux autres tables, puisque
/// `SessionStatus::Failed` porte un message, pas seulement un discriminant.
///
/// `upsert_session` ne touche jamais `created_at` lors d'un conflit : seul
/// l'`INSERT` initial le pose, `ON CONFLICT` ne met à jour que
/// `root_frame`/`status`/`last_updated_at` (voir la doc de
/// [`Session::created_at`]/[`Session::last_updated_at`]). `get`/`delete`
/// s'appuient sur `fetch_one` plutôt que `fetch_optional` : une session (ou
/// un frame) absent se traduit par `sqlx::Error::RowNotFound`, cohérent avec
/// les signatures `crate::Result<_>` (pas `Result<Option<_>, _>`) du trait.
///
/// Les clichés (`marie_session_snapshots`) sont couverts séparément par
/// `impl `[`StoreSessionSnapshot`]` for `[`PgStore`] (voir
/// `crate::session::snapshot::store`), `StoreSession` en hérite via son
/// supertrait.
#[async_trait]
impl StoreSession for PgStore {
    async fn get_session(&self, id: &SessionId) -> crate::Result<Session> {
        let row = sqlx::query(
            "SELECT id, root_frame, status, created_at, last_updated_at FROM marie_sessions WHERE id = $1",
        )
        .bind(id.to_string())
        .fetch_one(self.pool())
        .await?;

        Ok(decode_row(row)?)
    }

    async fn session_exists(&self, id: &SessionId) -> crate::Result<bool> {
        let row = sqlx::query("SELECT EXISTS(SELECT 1 FROM marie_sessions WHERE id = $1)")
            .bind(id.to_string())
            .fetch_one(self.pool())
            .await?;

        Ok(row.try_get::<bool, _>("exists")?)
    }

    async fn upsert_session(&self, session: Session) -> crate::Result<()> {
        sqlx::query(
            "INSERT INTO marie_sessions (id, root_frame, status, created_at, last_updated_at) \
             VALUES ($1, $2, $3, NOW(), NOW()) \
             ON CONFLICT (id) DO UPDATE SET root_frame = EXCLUDED.root_frame, status = EXCLUDED.status, last_updated_at = NOW()",
        )
        .bind(session.id.to_string())
        .bind(session.root_frame.map(|id| id.to_string()))
        .bind(Json(&session.status))
        .execute(self.pool())
        .await?;

        Ok(())
    }

    async fn delete_session(&self, id: &SessionId) -> crate::Result<Session> {
        let row = sqlx::query(
            "DELETE FROM marie_sessions WHERE id = $1 \
             RETURNING id, root_frame, status, created_at, last_updated_at",
        )
        .bind(id.to_string())
        .fetch_one(self.pool())
        .await?;

        Ok(decode_row(row)?)
    }

    async fn list_sessions(&self) -> crate::Result<Vec<Session>> {
        let rows = sqlx::query(
            "SELECT id, root_frame, status, created_at, last_updated_at FROM marie_sessions",
        )
        .fetch_all(self.pool())
        .await?;

        Ok(rows.into_iter().map(decode_row).collect::<Result<Vec<_>, _>>()?)
    }

    async fn get_session_view(&self, session_id: SessionId) -> crate::Result<SessionView> {
        let session = self.get_session(&session_id).await?;
        let logs = self.list_log(session_id).await?;
        let pendings = self.list_pending_hitls(session_id).await?;

        Ok(SessionView {
            id: session.id,
            status: session.status,
            logs,
            pendings
        })
    }
}

/// Reconstitue un [`Session`] depuis une ligne de `marie_sessions` —
/// symétrique de l'écriture dans [`StoreSession::upsert_session`]. `id` est
/// re-parsé depuis le `TEXT` stocké plutôt que d'être fiabilisé par le type
/// system : un échec ici voudrait dire que la colonne contient autre chose
/// qu'un `SessionId` que nous avons nous-mêmes écrit, ce qui ne peut pas
/// arriver en pratique (`id` est `PRIMARY KEY`, jamais écrit ailleurs que
/// [`StoreSession::upsert_session`]).
fn decode_row(row: PgRow) -> Result<Session, sqlx::Error> {
    Ok(Session {
        id: row
            .try_get::<String, _>("id")?
            .parse()
            .expect("l'id stocké dans marie_sessions est toujours un SessionId valide"),
        root_frame: row
            .try_get::<Option<String>, _>("root_frame")?
            .map(|s| s.parse().expect("le root_frame stocké dans marie_sessions est toujours un FrameId valide")),
        status: row.try_get::<Json<SessionStatus>, _>("status")?.0,
        created_at: row.try_get("created_at")?,
        last_updated_at: row.try_get("last_updated_at")?,
    })
}

/// Erreur renvoyée par [`InMemorySessionStore`] — seul cas où
/// [`StoreSession`] peut échouer sans backend externe : une absence, jamais
/// une panne de connexion/désérialisation comme côté [`PgStore`].
#[derive(Debug, thiserror::Error)]
pub enum InMemorySessionStoreError {
    #[error("session {0} introuvable")]
    SessionNotFound(SessionId),
}

/// Implémentation en mémoire de [`StoreSession`] — même rôle que
/// [`crate::catalog::in_memory::InMemoryCatalog`] vis-à-vis de
/// [`crate::catalog::store::StoreCatalog`] : pas de dépendance à Postgres,
/// pour tester `SessionController`/`FrameTree`/`Snapshots`/`Hitls` sans base
/// de données. Chaque table Postgres devient une `HashMap` indexée par la
/// même clé primaire composite que sa table d'origine (voir la doc de
/// [`PgStore`]'s `impl StoreSession`) sous un `parking_lot::Mutex` — les
/// sections critiques ne contiennent jamais de `.await`, donc pas de risque
/// d'inversion de priorité/deadlock vis-à-vis de l'exécuteur tokio.
#[derive(Default)]
pub struct InMemorySessionStore {
    sessions: Mutex<HashMap<SessionId, Session>>,
    /// Instance de [`crate::session::frames::store::InMemorySessionFrameStore`]
    /// plutôt qu'une `HashMap` brute : `impl `[`StoreSessionFrame`]` for
    /// `[`InMemorySessionStore`] délègue entièrement à ce type (voir sa
    /// doc dans `crate::session::frames::store`), pour ne pas dupliquer la
    /// logique `is_root` entre les deux.
    pub(crate) frames: crate::session::frames::store::InMemorySessionFrameStore,
    /// Instance de [`InMemorySessionSnapshotStore`] plutôt qu'une `HashMap`
    /// brute : `impl `[`StoreSessionSnapshot`]` for `[`InMemorySessionStore`]
    /// (dans `crate::session::snapshot::store`, pas dans ce module — voir sa
    /// doc) délègue entièrement à ce type, pour ne pas dupliquer la logique
    /// entre les deux.
    pub(crate) snapshots: InMemorySessionSnapshotStore,
    /// Instance de [`InMemoryHitlStore`] plutôt qu'une `HashMap` brute :
    /// `impl `[`StoreHitl`]` for `[`InMemorySessionStore`] (dans
    /// `crate::hitl::store`, pas dans ce module — voir sa doc) délègue
    /// entièrement à ce type, pour ne pas dupliquer la logique entre les
    /// deux.
    pub(crate) hitls: InMemoryHitlStore,
    /// Instance de [`InMemorySessionLogsStore`] plutôt qu'une `HashMap`
    /// brute : `impl `[`StoreSessionLogs`]` for `[`InMemorySessionStore`]
    /// (dans `crate::session::logs::store`, pas dans ce module — voir sa
    /// doc) délègue entièrement à ce type, pour ne pas dupliquer la logique
    /// entre les deux.
    pub(crate) logs: InMemorySessionLogsStore,
}

impl InMemorySessionStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StoreSession for InMemorySessionStore {
    async fn get_session(&self, id: &SessionId) -> crate::Result<Session> {
        Ok(self.sessions.lock().get(id).cloned().ok_or(InMemorySessionStoreError::SessionNotFound(*id))?)
    }

    async fn session_exists(&self, id: &SessionId) -> crate::Result<bool> {
        Ok(self.sessions.lock().contains_key(id))
    }

    async fn upsert_session(&self, session: Session) -> crate::Result<()> {
        self.sessions.lock().insert(session.id, session);
        Ok(())
    }

    async fn delete_session(&self, id: &SessionId) -> crate::Result<Session> {
        Ok(self.sessions.lock().remove(id).ok_or(InMemorySessionStoreError::SessionNotFound(*id))?)
    }

    async fn list_sessions(&self) -> crate::Result<Vec<Session>> {
        Ok(self.sessions.lock().values().cloned().collect())
    }

    async fn get_session_view(&self, session_id: SessionId) -> crate::Result<SessionView> {
        let session = self.get_session(&session_id).await?;
        let logs = self.list_log(session_id).await?;
        let pendings = self.list_pending_hitls(session_id).await?;

        Ok(SessionView {
            id: session.id,
            status: session.status,
            logs,
            pendings
        })
    }
}
