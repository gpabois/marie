use futures::{SinkExt, StreamExt, stream::BoxStream};

use crate::{layer::{Layer, LayerChain}, pubsub::PubSubMessage, rpc::register::RpcEvent, sink::{BoxSink, SinkBoxExt as _}};

pub struct RpcEventLayer(<Self as Layer>::Sender, <Self as Layer>::Receiver);

impl Layer for RpcEventLayer {
    type Send = RpcEvent;
    type Received = RpcEvent;
    type Sender = BoxSink<'static, Self::Send, anyhow::Error>;
    type Receiver = BoxStream<'static, Self::Received>;

    fn split(self) -> (Self::Sender, Self::Receiver) {
        (self.0, self.1)
    }
}

impl<L> LayerChain<L, ()> for RpcEventLayer where L: Layer<Send=PubSubMessage, Received=PubSubMessage> {
    
    fn chain(layer: L, args: ()) -> Self {
        let (tx, rx) = layer.split();

        let tx = tx.with(|event: RpcEvent| {
            std::future::ready(Ok(PubSubMessage {
                source: event.source,
                topic: event.topic(),
                payload: serde_json::to_vec(&event.kind).unwrap()
            }))
        }).boxed_sink();

        let rx = rx.filter_map(|msg| {
            if msg.topic.starts_with("rpc/events") {
                let kind = serde_json::from_slice(&msg.payload).unwrap();
                std::future::ready(Some(RpcEvent {
                    source: msg.source,
                    kind
                }))
            } else {
                std::future::ready(None)
            }
        }).boxed();

        Self(tx, rx)
    }
}