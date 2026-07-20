use futures::{SinkExt as _, StreamExt as _, stream::BoxStream};

use crate::{
    layer::{IntoService, Layer, LayerChain},
    pubsub::PubSubMessage,
    session::{
        SessionEvent,
        server::{SessionServer, SessionServerActor, SessionServerArgs},
    },
    sink::{BoxSink, SinkBoxExt as _},
};

pub struct SessionEventLayer(<Self as Layer>::Sender, <Self as Layer>::Receiver);

impl Layer for SessionEventLayer {
    type Send = SessionEvent;
    type Received = SessionEvent;
    type Sender = BoxSink<'static, Self::Send, anyhow::Error>;
    type Receiver = BoxStream<'static, Self::Received>;

    fn split(self) -> (Self::Sender, Self::Receiver) {
        (self.0, self.1)
    }
}

impl<T> IntoService<SessionServer, SessionServerArgs> for T
    where
        T: Layer<Send = SessionEvent, Received = SessionEvent>,
{

    fn into_service(self, args: SessionServerArgs) -> SessionServer {
        SessionServerActor::create(self, args)
    }
}

impl<L> LayerChain<L, ()> for SessionEventLayer where L: Layer<Send=PubSubMessage, Received=PubSubMessage> {

    /// Chaque [`SessionEvent`] est publié deux fois — sur son topic dédié
    /// (voir [`SessionEvent::topic`]) et sur le topic global (voir
    /// [`SessionEvent::global_topic`]) — pour servir aussi bien un abonné
    /// intéressé par une seule session qu'un abonné voulant tout le cycle de
    /// vie sans connaître les identifiants de session à l'avance.
    fn chain(layer: L, _: ()) -> Self {
        let (tx, rx) = layer.split();

        let tx = tx.with_flat_map(|event: SessionEvent| {
            let payload = serde_json::to_vec(&event).unwrap();

            let dedicated = PubSubMessage { id: String::default(), source: None, topic: event.topic(), payload: payload.clone() };
            let global = PubSubMessage { id: String::default(), source: None, topic: event.global_topic(), payload };

            futures::stream::iter([Ok(dedicated), Ok(global)])
        }).boxed_sink();

        let rx = rx.filter_map(|msg| {
            if msg.topic.starts_with(SessionEvent::TOPIC_PREFIX) {
                let event: SessionEvent = serde_json::from_slice(&msg.payload).unwrap();
                std::future::ready(Some(event))
            } else {
                std::future::ready(None)
            }
        }).boxed();

        Self(tx, rx)
    }
}
