use tokio::sync::mpsc;

use crate::{di::{Constructible, Factory, Get, Resolve}, graph::Graphs, hitl::service::SessionHitlsFactory, id::IdGenerator, session::{Session, frames::FrameTreeFactory, logs::SessionLogsFactory, protocol::SessionCheckpointEvent, snapshot::SessionSnapshotsFactory, store::SessionStore}, worker::WorkerClient};

use super::{SessionCheckpointer, SessionCheckpointerArgs};

pub type SessionCheckpointerFactory = Factory<SessionCheckpointer, (Session, mpsc::UnboundedSender<SessionCheckpointEvent>)>;

impl<C> Constructible<C> for SessionCheckpointerFactory 
    where C: Clone + Send + Sync + 'static
            + Get<SessionStore>
            + Resolve<WorkerClient>
            + Resolve<Graphs>
            + Resolve<SessionStore>
            + Resolve<SessionHitlsFactory>
            + Resolve<SessionSnapshotsFactory>
            + Resolve<SessionLogsFactory>
            + Resolve<FrameTreeFactory>
            + Resolve<IdGenerator>
{
    fn construct(container: &C, args: ()) -> Self {
        let container = container.clone();
        
        let session_hitls_factory: SessionHitlsFactory = container.resolve(());
        let session_snapshots_factory: SessionSnapshotsFactory = container.resolve(());
        let session_logs_factory: SessionLogsFactory = container.resolve(());
        let frame_tree_factory: FrameTreeFactory = container.resolve(());

        Self::new(move |(session, queue)| {
            let session_hitls = session_hitls_factory.create(session.id);
            let session_snapshots = session_snapshots_factory.create(session.id);
            let session_logs = session_logs_factory.create(session.id);
            let frame_tree = frame_tree_factory.create(session.id);
            
            let args = SessionCheckpointerArgs::builder()
                .session(session)
                .id(container.resolve(()))
                .queue(queue)
                .worker(container.resolve(()))
                .graphs(container.resolve(()))
                .session_logs(session_logs)
                .hitls(session_hitls)
                .store(container.get())
                .snapshots(session_snapshots)
                .frames(frame_tree)
                .build();

            SessionCheckpointer::new(args)
        })
    }
}
