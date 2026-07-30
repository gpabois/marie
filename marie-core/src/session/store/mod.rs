use async_trait::async_trait;
use sqlx::Row as _;
use sqlx::postgres::PgRow;
use sqlx::types::Json;

use crate::session::frames::{FrameId, FrameNode};
use crate::session::snapshot::Snapshot;
use crate::session::{Session, SessionId, SessionStatus};
use crate::store::PgStore;

#[async_trait]
pub trait SessionStore {
    type Error;

    async fn get_session(&self, id: &SessionId) -> Result<Session, Self::Error>;
    async fn upsert_session(&self, session: Session) -> Result<(), Self::Error>;
    async fn delete_session(&self, id: &SessionId) -> Result<Session, Self::Error>;

    async fn get_frame(&self, id: &SessionId, frame_id: &FrameId) -> Result<FrameNode, Self::Error>;
    async fn upsert_frame(&self, node: FrameNode) -> Result<FrameNode, Self::Error>;
    async fn delete_frame(&self, id: &SessionId, frame_id: &FrameId) -> Result<FrameNode, Self::Error>;

    async fn latest_snapshot(&self, id: &SessionId, frame_id: &FrameId) -> Result<Snapshot, Self::Error>;
    async fn snapshot_at(&self, id: &SessionId, frame_id: &FrameId, superstep: u32) -> Result<Snapshot, Self::Error>;
    async fn upsert_snapshot(&self, snapshot: Snapshot) -> Result<(), Self::Error>;
}

/// Implémentation PostgreSQL de [`SessionStore`], contre les tables
/// `marie_sessions` (voir `migrations/0002_session.sql`,
/// `migrations/0017_session_root_frame.sql` et
/// `migrations/0018_session_status.sql`) et `marie_session_frames` (voir
/// `migrations/0015_session_frame.sql`) — même poignée [`PgStore`] que
/// `catalog`/`workspace` (voir leurs `store.rs` respectifs).
///
/// `marie_sessions` ne porte plus que `root_frame` (voir
/// [`Session::root_frame`]), pas l'arbre entier : depuis
/// `migrations/0015_session_frame.sql`, chaque frame vit dans sa propre
/// ligne de `marie_session_frames`, adressée séparément par
/// [`Self::get_frame`]/[`Self::upsert_frame`]/[`Self::delete_frame`].
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
/// les signatures `Result<_, Self::Error>` (pas `Result<Option<_>, _>`) du
/// trait.
///
/// `get_frame`/`upsert_frame`/`delete_frame` adressent `marie_session_frames`
/// par sa clé primaire composite `(session_id, frame_id)` : un `FrameId`
/// n'est unique qu'au sein de sa session, donc c'est la paire — pas
/// `frame_id` seul — que `ON CONFLICT` doit cibler pour que deux sessions
/// distinctes puissent réutiliser des `FrameId` sans collision (même logique
/// que `fs_alias.(scope, from_path)`, voir `vfs::alias::PostgresAliasCatalog`).
///
/// `latest_snapshot`/`snapshot_at`/`upsert_snapshot` adressent
/// `marie_session_snapshots` (voir `migrations/0016_session_snapshot.sql`)
/// par sa clé primaire composite `(session_id, frame_id, superstep)` :
/// plusieurs clichés coexistent pour un même frame (un par `superstep`), donc
/// c'est ce triplet — pas `(session_id, frame_id)` — qui identifie un
/// cliché. `superstep` (`u32` côté domaine) est lié en `i64` : Postgres n'a
/// pas d'entier non signé (voir `sqlx::postgres::types`).
#[async_trait]
impl SessionStore for PgStore {
    type Error = sqlx::Error;

    async fn get_session(&self, id: &SessionId) -> Result<Session, Self::Error> {
        let row = sqlx::query(
            "SELECT id, root_frame, status, created_at, last_updated_at FROM marie_sessions WHERE id = $1",
        )
        .bind(id.to_string())
        .fetch_one(self.pool())
        .await?;

        decode_row(row)
    }

    async fn upsert_session(&self, session: Session) -> Result<(), Self::Error> {
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

    async fn delete_session(&self, id: &SessionId) -> Result<Session, Self::Error> {
        let row = sqlx::query(
            "DELETE FROM marie_sessions WHERE id = $1 \
             RETURNING id, root_frame, status, created_at, last_updated_at",
        )
        .bind(id.to_string())
        .fetch_one(self.pool())
        .await?;

        decode_row(row)
    }

    async fn get_frame(&self, id: &SessionId, frame_id: &FrameId) -> Result<FrameNode, Self::Error> {
        let row = sqlx::query(
            "SELECT node FROM marie_session_frames WHERE session_id = $1 AND frame_id = $2",
        )
        .bind(id.to_string())
        .bind(frame_id.to_string())
        .fetch_one(self.pool())
        .await?;

        Ok(row.try_get::<Json<FrameNode>, _>("node")?.0)
    }

    async fn upsert_frame(&self, node: FrameNode) -> Result<FrameNode, Self::Error> {
        sqlx::query(
            "INSERT INTO marie_session_frames (session_id, frame_id, node, created_at, last_updated_at) \
             VALUES ($1, $2, $3, NOW(), NOW()) \
             ON CONFLICT (session_id, frame_id) DO UPDATE SET node = EXCLUDED.node, last_updated_at = NOW()",
        )
        .bind(node.session_id.to_string())
        .bind(node.id.to_string())
        .bind(Json(&node))
        .execute(self.pool())
        .await?;

        Ok(node)
    }

    async fn delete_frame(&self, id: &SessionId, frame_id: &FrameId) -> Result<FrameNode, Self::Error> {
        let row = sqlx::query(
            "DELETE FROM marie_session_frames WHERE session_id = $1 AND frame_id = $2 \
             RETURNING node",
        )
        .bind(id.to_string())
        .bind(frame_id.to_string())
        .fetch_one(self.pool())
        .await?;

        Ok(row.try_get::<Json<FrameNode>, _>("node")?.0)
    }

    async fn latest_snapshot(&self, id: &SessionId, frame_id: &FrameId) -> Result<Snapshot, Self::Error> {
        let row = sqlx::query(
            "SELECT data FROM marie_session_snapshots \
             WHERE session_id = $1 AND frame_id = $2 \
             ORDER BY superstep DESC LIMIT 1",
        )
        .bind(id.to_string())
        .bind(frame_id.to_string())
        .fetch_one(self.pool())
        .await?;

        Ok(row.try_get::<Json<Snapshot>, _>("data")?.0)
    }

    async fn snapshot_at(&self, id: &SessionId, frame_id: &FrameId, superstep: u32) -> Result<Snapshot, Self::Error> {
        let row = sqlx::query(
            "SELECT data FROM marie_session_snapshots \
             WHERE session_id = $1 AND frame_id = $2 AND superstep = $3",
        )
        .bind(id.to_string())
        .bind(frame_id.to_string())
        .bind(superstep as i64)
        .fetch_one(self.pool())
        .await?;

        Ok(row.try_get::<Json<Snapshot>, _>("data")?.0)
    }

    async fn upsert_snapshot(&self, snapshot: Snapshot) -> Result<(), Self::Error> {
        sqlx::query(
            "INSERT INTO marie_session_snapshots (session_id, frame_id, superstep, data, created_at) \
             VALUES ($1, $2, $3, $4, NOW()) \
             ON CONFLICT (session_id, frame_id, superstep) DO UPDATE SET data = EXCLUDED.data",
        )
        .bind(snapshot.session_id.to_string())
        .bind(snapshot.frame_id.to_string())
        .bind(snapshot.superstep as i64)
        .bind(Json(&snapshot))
        .execute(self.pool())
        .await?;

        Ok(())
    }
}

/// Reconstitue un [`Session`] depuis une ligne de `marie_sessions` —
/// symétrique de l'écriture dans [`SessionStore::upsert`]. `id` est
/// re-parsé depuis le `TEXT` stocké plutôt que d'être fiabilisé par le type
/// system : un échec ici voudrait dire que la colonne contient autre chose
/// qu'un `SessionId` que nous avons nous-mêmes écrit, ce qui ne peut pas
/// arriver en pratique (`id` est `PRIMARY KEY`, jamais écrit ailleurs que
/// [`SessionStore::upsert`]).
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