use futures::channel::oneshot;

use crate::session::{Session, SessionId};

pub enum Command {
    GetSession(SessionId, oneshot::Sender<Result<Option<Session>, anyhow::Error>>),
    ListSessions(oneshot::Sender<Result<Vec<Session>, anyhow::Error>>),
    InsertSession(Session, oneshot::Sender<Result<(), anyhow::Error>>),
    ReplaceSession(Session, oneshot::Sender<Result<(), anyhow::Error>>),
    DeleteSession(SessionId, oneshot::Sender<Result<(), anyhow::Error>>),
    Shutdown
}
