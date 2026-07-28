use async_trait::async_trait;
use sqlx::Row as _;
use sqlx::postgres::PgRow;
use sqlx::types::Json;

use crate::session::frames::FrameTree;
use crate::session::{Session, SessionId};
use crate::store::PgStore;

#[async_trait]
pub trait SessionStore {
    type Error;

    async fn get_session(&self, id: &SessionId) -> Result<Session, Self::Error>;
    async fn upsert_session(&self, session: Session) -> Result<(), Self::Error>;
    async fn delete_session(&self, id: &SessionId) -> Result<Session, Self::Error>;
}

/// Implémentation PostgreSQL de [`SessionStore`], contre la table
/// `marie_sessions` (voir `migrations/0002_session.sql` et
/// `migrations/0014_session_tree.sql`) — même poignée [`PgStore`] que
/// `catalog`/`workspace` (voir leurs `store.rs` respectifs).
///
/// `upsert` ne touche jamais `created_at` lors d'un conflit : seul l'`INSERT`
/// initial le pose, `ON CONFLICT` ne met à jour que `tree`/`last_updated_at`
/// (voir la doc de [`Session::created_at`]/[`Session::last_updated_at`]).
/// `get`/`delete` s'appuient sur `fetch_one` plutôt que `fetch_optional` :
/// une session absente se traduit par `sqlx::Error::RowNotFound`, cohérent
/// avec la signature `Result<Session, Self::Error>` (pas
/// `Result<Option<Session>, _>`) du trait.
#[async_trait]
impl SessionStore for PgStore {
    type Error = sqlx::Error;

    async fn get_session(&self, id: &SessionId) -> Result<Session, Self::Error> {
        let row = sqlx::query(
            "SELECT id, tree, created_at, last_updated_at FROM marie_sessions WHERE id = $1",
        )
        .bind(id.to_string())
        .fetch_one(self.pool())
        .await?;

        decode_row(row)
    }

    async fn upsert_session(&self, session: Session) -> Result<(), Self::Error> {
        sqlx::query(
            "INSERT INTO marie_sessions (id, frames, created_at, last_updated_at) \
             VALUES ($1, $2, NOW(), NOW()) \
             ON CONFLICT (id) DO UPDATE SET frames = EXCLUDED.tree, last_updated_at = NOW()",
        )
        .bind(session.id.to_string())
        .bind(Json(&session.frames))
        .execute(self.pool())
        .await?;

        Ok(())
    }

    async fn delete_session(&self, id: &SessionId) -> Result<Session, Self::Error> {
        let row = sqlx::query(
            "DELETE FROM marie_sessions WHERE id = $1 \
             RETURNING id, frames, created_at, last_updated_at",
        )
        .bind(id.to_string())
        .fetch_one(self.pool())
        .await?;

        decode_row(row)
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
        frames: row.try_get::<Json<FrameTree>, _>("frames")?.0,
        created_at: row.try_get("created_at")?,
        last_updated_at: row.try_get("last_updated_at")?,
    })
}