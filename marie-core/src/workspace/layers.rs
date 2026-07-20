use futures::{SinkExt as _, StreamExt as _, stream::BoxStream};

use crate::{
    layer::{IntoService, Layer, LayerChain},
    pubsub::PubSubMessage,
    sink::{BoxSink, SinkBoxExt as _},
    workspace::{
        WorkspaceEvent,
        server::{WorkspaceServer, WorkspaceServerActor, WorkspaceServerArgs},
    },
};

pub struct WorkspaceEventLayer(<Self as Layer>::Sender, <Self as Layer>::Receiver);

impl Layer for WorkspaceEventLayer {
    type Send = WorkspaceEvent;
    type Received = WorkspaceEvent;
    type Sender = BoxSink<'static, Self::Send, anyhow::Error>;
    type Receiver = BoxStream<'static, Self::Received>;

    fn split(self) -> (Self::Sender, Self::Receiver) {
        (self.0, self.1)
    }
}

impl<T> IntoService<WorkspaceServer, WorkspaceServerArgs> for T
    where
        T: Layer<Send = WorkspaceEvent, Received = WorkspaceEvent>,
{

    fn into_service(self, args: WorkspaceServerArgs) -> WorkspaceServer {
        WorkspaceServerActor::create(self, args)
    }
}

impl<L> LayerChain<L, ()> for WorkspaceEventLayer where L: Layer<Send=PubSubMessage, Received=PubSubMessage> {

    /// Chaque [`WorkspaceEvent`] est publié deux fois — sur son topic dédié
    /// (voir [`WorkspaceEvent::topic`]) et sur le topic global (voir
    /// [`WorkspaceEvent::global_topic`]) — pour servir aussi bien un abonné
    /// intéressé par un seul workspace (ex. une passerelle qui relaie les
    /// évènements d'un workspace donné à un client WebSocket) qu'un abonné
    /// voulant tout le cycle de vie sans connaître les identifiants de
    /// workspace à l'avance — même mécanique que
    /// [`crate::session::layers::SessionEventLayer`].
    fn chain(layer: L, _: ()) -> Self {
        let (tx, rx) = layer.split();

        let tx = tx.with_flat_map(|event: WorkspaceEvent| {
            let payload = serde_json::to_vec(&event).unwrap();

            let dedicated = PubSubMessage { id: String::default(), source: None, topic: event.topic(), payload: payload.clone() };
            let global = PubSubMessage { id: String::default(), source: None, topic: event.global_topic(), payload };

            futures::stream::iter([Ok(dedicated), Ok(global)])
        }).boxed_sink();

        let rx = rx.filter_map(|msg| {
            if msg.topic.starts_with(WorkspaceEvent::TOPIC_PREFIX) {
                let event: WorkspaceEvent = serde_json::from_slice(&msg.payload).unwrap();
                std::future::ready(Some(event))
            } else {
                std::future::ready(None)
            }
        }).boxed();

        Self(tx, rx)
    }
}
