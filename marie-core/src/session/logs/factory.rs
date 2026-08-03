use crate::{di::{Constructible, Factory, Get, Resolve}, events::EventBus, id::IdGenerator, session::{SessionId, logs::{SessionLogs, SessionLogsArgs, store::SessionLogStore}}};

pub type SessionLogsFactory = Factory<SessionLogs, SessionId>;

impl<C> Constructible<C> for SessionLogsFactory 
    where C: Get<SessionLogStore> 
            + Resolve<EventBus>
            + Resolve<IdGenerator>
            + Clone + Send + Sync + 'static
{
    fn construct(container: &C, _: ()) -> Self {
        let container = container.clone();

        Self::new(move |session_id| {
            let args = SessionLogsArgs::builder()
                .id(container.resolve(()))
                .store(container.get())
                .session_id(session_id)
                .bus(container.resolve(()))
                .build();

            SessionLogs::new(args)
        })
    }
}