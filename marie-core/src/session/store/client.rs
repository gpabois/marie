use std::sync::Arc;

use async_trait::async_trait;
use tokio::select;
use tokio::sync::{mpsc, oneshot};

use crate::session::{Session, SessionId};

use super::protocol::Command::{*, self};
use super::SessionStore;

struct Actor;

impl Actor {
    pub fn create<Store>(store: Store) -> SessionStoreClient where Store: SessionStore + 'static {
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<Command>();
        
        let stor = store.clone();
        
        tokio::spawn(async move {
            let store = stor;
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