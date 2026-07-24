use futures::{SinkExt, StreamExt, stream::BoxStream};
use libp2p::gossipsub::Topic;

use crate::{layer::{Layer, LayerChain}, network::protocol::{NetworkCommand, NetworkEvent}, pubsub::PubSubMessage, sink::{BoxSink, SinkBoxExt}};

pub struct PubSubLayer(<Self as Layer>::Sender, <Self as Layer>::Receiver);

impl Layer for PubSubLayer {
    type Send = PubSubMessage;
    type Received = PubSubMessage;

    type Sender = BoxSink<'static, Self::Send, anyhow::Error>;
    type Receiver = BoxStream<'static, Self::Received>;

    fn split(self) -> (Self::Sender, Self::Receiver) {
        (self.0, self.1)
    }
}

impl<L> LayerChain<L, ()> for PubSubLayer where L: Layer<Send=NetworkCommand, Received = NetworkEvent> {
    fn chain(layer: L, _: ()) -> Self {
        Self::new(layer)
    }
}

impl PubSubLayer {
    pub fn new(layer: impl Layer<Send=NetworkCommand, Received = NetworkEvent>) -> Self {
        use NetworkCommand::Publish;
        let (tx, rx) = layer.split();

        let tx = tx.with(|msg: PubSubMessage| {
            std::future::ready(Ok(Publish {
                topic: Topic::new(msg.topic),
                payload: msg.payload
            }))
        }).boxed_sink();

        let rx = rx.filter_map(|event| {
            match event {
                NetworkEvent::PubSubReceived { id, topic, data: payload, source } => {
                    std::future::ready(Some(PubSubMessage {
                        id,
                        topic,
                        payload,
                        source: Some(source)
                    }))
                },
                _ => std::future::ready(None)
            }
        }).boxed();

        Self(tx, rx)

    }
}

pub struct FilterPubSub(<Self as Layer>::Sender, <Self as Layer>::Receiver);

impl<L> LayerChain<L, &'static [&str]> for FilterPubSub where L: Layer<Send = PubSubMessage, Received = PubSubMessage> {
    fn chain(layer: L, topics: &'static [&str]) -> Self {
        Self::new(layer, topics)
    }
}

impl Layer for FilterPubSub {
    type Send = PubSubMessage;
    type Received = PubSubMessage;
    type Sender = BoxSink<'static, Self::Send, anyhow::Error>;
    type Receiver = BoxStream<'static, Self::Received>;

    fn split(self) -> (Self::Sender, Self::Receiver) {
        (self.0, self.1)
    }
}

impl FilterPubSub {
    pub fn new(layer: impl Layer<Send=PubSubMessage, Received = PubSubMessage>, topics: &'static [&'static str]) -> Self {
        let (tx, rx) = layer.split();

        let rx = rx.filter_map(move |msg| {
            if topics.contains(&msg.topic.as_str()) {
                std::future::ready(Some(msg))
            } else {
                std::future::ready(None)
            }
        }).boxed();

        Self(tx.boxed_sink(), rx)
    }
}