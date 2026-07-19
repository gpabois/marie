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
        SessionServerActor::new(self, args)
    }
}

impl<L> LayerChain<L, ()> for SessionEventLayer where L: Layer<Send=PubSubMessage, Received=PubSubMessage> {

    fn chain(layer: L, _: ()) -> Self {
        let (tx, rx) = layer.split();

        let tx = tx.with(|event: SessionEvent| {
            std::future::ready(Ok(PubSubMessage {
                id: String::default(),
                source: None,
                topic: event.topic(),
                payload: serde_json::to_vec(&event).unwrap()
            }))
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
