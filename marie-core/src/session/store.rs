use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tokio::select;

use crate::session::{Session, SessionId};

#[cfg(feature = "catalog")]
use crate::{
    agent::{AgentId, frame::AgentFrame},
    session::{
        SessionLog,
        state::{frame::GraphFrame, hitl::HitlFrame, orchestration::OrchestrationFrame},
    },
    store::PgStore,
};
#[cfg(feature = "catalog")]
use chrono::{DateTime, Utc};
#[cfg(feature = "catalog")]
use sqlx::Row as _;
#[cfg(feature = "catalog")]
use sqlx::postgres::PgRow;
#[cfg(feature = "catalog")]
use sqlx::types::Json;


enum Command {
    GetSession(SessionId, oneshot::Sender<Result<Option<Session>, anyhow::Error>>),
    ListSessions(oneshot::Sender<Result<Vec<Session>, anyhow::Error>>),
    InsertSession(Session, oneshot::Sender<Result<(), anyhow::Error>>),
    ReplaceSession(Session, oneshot::Sender<Result<(), anyhow::Error>>),
    DeleteSession(SessionId, oneshot::Sender<Result<(), anyhow::Error>>),
    Shutdown
}

pub struct SessionStoreActor;

impl SessionStoreActor {
    pub fn create<Store>(store: Store) -> SessionStoreClient where Store: SessionStore + 'static {
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<Command>();
        
        let stor = store.clone();
        
        tokio::spawn(async move {
            let store = stor;
            use Command::*;
            loop {
                select! {
                    Some(cmd) = cmd_rx.recv() => {
                        match cmd {
                            GetSession(id, to) => {
                                to.send(store.clone().get(id).await);
                            },
                            ListSessions(to) => {
                                to.send(store.clone().list().await);
                            }
                            InsertSession(session, to) => {
                                to.send(store.clone().insert(session).await);
                            },
                            ReplaceSession(session, to) => {
                                to.send(store.clone().replace(session).await);
                            },
                            DeleteSession(session_id, to) => {
                                to.send(store.clone().delete(session_id).await);
                            },
                            Shutdown => break
                        }
                    }
                }
            }
        });

        SessionStoreClient(cmd_tx.clone(), Arc::new(Handler(cmd_tx)))
    } 
}

struct Handler(mpsc::UnboundedSender<Command>);

impl Drop for Handler {
    fn drop(&mut self) {
        self.0.send(Command::Shutdown);
    }
}

/// Client du stockage de session
#[derive(Clone)]
pub struct SessionStoreClient(mpsc::UnboundedSender<Command>, Arc<Handler>);

#[async_trait]
impl SessionStore for SessionStoreClient {
    async fn get(self, id: SessionId) -> anyhow::Result<Option<Session>> {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::GetSession(id, tx))?;
        rx.await?
    }

    async fn insert(mut self, session: Session) -> anyhow::Result<()>
    {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::InsertSession(session, tx));
        rx.await?
    }
    async fn replace(mut self, session: Session) -> anyhow::Result<()>
    {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::ReplaceSession(session, tx));
        rx.await?
    }
    async fn delete(mut self, id: SessionId) -> anyhow::Result<()>
    {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::DeleteSession(id, tx));
        rx.await?
    }

    async fn list(self) -> anyhow::Result<Vec<Session>>
    {
        let (tx, rx) = oneshot::channel();
        self.0.send(Command::ListSessions(tx));
        rx.await?
    }
}

/// Stockage CRUD d'une [`Session`] complète (voir `session::model::Session`)
/// — implémenté directement pour [`PgStore`] ci-dessous plutôt que dérivé
/// d'un trait CRUD générique, même principe que
/// `persistency::workspace::WorkspaceStore`/`expert::catalog::store::ExpertStore`.
///
/// [`Self::insert`] et [`Self::replace`] sont volontairement deux méthodes
/// distinctes (pas un simple upsert comme `WorkspaceStore::put`) : elles
/// portent la sémantique de `session::rpc::InsertSession`/`UpdateSession`
/// ("crée" contre "remplace l'état complet d'une session *existante*"), et
/// c'est ce qui permet à l'implémentation Postgres de ne poser
/// `Session::created_at` qu'à la création et de ne jamais y retoucher lors
/// d'un remplacement — voir la doc de ces deux champs.
#[async_trait]
pub trait SessionStore: Send + Sync + Clone {
    async fn get(self, id: SessionId) -> anyhow::Result<Option<Session>>;
    async fn insert(mut self, session: Session) -> anyhow::Result<()>;
    async fn replace(mut self, session: Session) -> anyhow::Result<()>;
    async fn delete(mut self, id: SessionId) -> anyhow::Result<()>;
    async fn list(self) -> anyhow::Result<Vec<Session>>;
}

/// Reconstitue une [`Session`] depuis une ligne de la table `session` (voir
/// `migrations/0001_session.sql`) — symétrique de l'insertion dans
/// [`PgStore::insert`]/[`PgStore::replace`]. Chaque collection de [`Session`]
/// a sa propre colonne JSONB plutôt qu'un blob unique : contrairement à
/// l'ancienne table `session` (contenu CRDT `yrs`, voir
/// `persistency::session`), cette `Session`-ci est un enregistrement
/// classique remplacé en bloc à chaque mutation, donc décomposable colonne à
/// colonne comme `expert`/`model`/`tool`.
#[cfg(feature = "catalog")]
fn decode_row(row: PgRow) -> anyhow::Result<Session> {
    Ok(Session {
        id: row.try_get::<String, _>("id")?.parse()?,
        frames: row.try_get::<Json<std::collections::HashMap<AgentId, AgentFrame>>, _>("frames")?.0.into(),
        graphs: row.try_get::<Json<std::collections::HashMap<_, GraphFrame>>, _>("graphs")?.0,
        orchestrations: row.try_get::<Json<std::collections::HashMap<_, OrchestrationFrame>>, _>("orchestrations")?.0,
        hitls: row.try_get::<Json<std::collections::HashMap<_, HitlFrame>>, _>("hitls")?.0,
        logs: row.try_get::<Json<Vec<SessionLog>>, _>("logs")?.0,
        vars: row.try_get::<Json<std::collections::HashMap<String, serde_json::Value>>, _>("vars")?.0,
        created_at: row.try_get::<DateTime<Utc>, _>("created_at")?,
        last_updated_at: row.try_get::<DateTime<Utc>, _>("last_updated_at")?,
    })
}

#[cfg(feature = "catalog")]
#[async_trait]
impl SessionStore for PgStore {
    async fn list(self) -> anyhow::Result<Vec<Session>> {
        let rows = sqlx::query(
            "SELECT id, frames, graphs, orchestrations, hitls, logs, vars, created_at, last_updated_at \
             FROM session",
        )
        .fetch_all(self.pool())
        .await?;
    
        rows.into_iter().map(decode_row).collect()  
    }

    async fn get(self, id: SessionId) -> anyhow::Result<Option<Session>> {
        let id = id.to_string();
        let row = sqlx::query(
            "SELECT id, frames, graphs, orchestrations, hitls, logs, vars, created_at, last_updated_at \
             FROM session WHERE id = $1",
        )
        .bind(&id)
        .fetch_optional(self.pool())
        .await?;

        row.map(decode_row).transpose()
    }

    async fn insert(mut self, session: Session) -> anyhow::Result<()> {
        let id = session.id.to_string();

        sqlx::query(
            "INSERT INTO session (id, frames, graphs, orchestrations, hitls, logs, vars, created_at, last_updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())",
        )
        .bind(&id)
        .bind(Json(&session.frames))
        .bind(Json(&session.graphs))
        .bind(Json(&session.orchestrations))
        .bind(Json(&session.hitls))
        .bind(Json(&session.logs))
        .bind(Json(&session.vars))
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn replace(mut self, session: Session) -> anyhow::Result<()> {
        let id = session.id.to_string();

        sqlx::query(
            "UPDATE session SET \
                frames = $2, graphs = $3, orchestrations = $4, hitls = $5, logs = $6, vars = $7, last_updated_at = NOW() \
             WHERE id = $1",
        )
        .bind(&id)
        .bind(Json(&session.frames))
        .bind(Json(&session.graphs))
        .bind(Json(&session.orchestrations))
        .bind(Json(&session.hitls))
        .bind(Json(&session.logs))
        .bind(Json(&session.vars))
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn delete(mut self, id: SessionId) -> anyhow::Result<()> {
        let id = id.to_string();
        sqlx::query("DELETE FROM session WHERE id = $1").bind(&id).execute(self.pool()).await?;
        Ok(())
    }
}
