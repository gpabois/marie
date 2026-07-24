use futures::{FutureExt, SinkExt, StreamExt, stream::BoxStream};

use crate::{layer::{Layer, LayerChain}, worker::WorkerEvent, pubsub::PubSubMessage, sink::{BoxSink, SinkBoxExt}, tools::ToolEvent};

pub struct ToolEventLayer(<Self as Layer>::Sender, <Self as Layer>::Receiver);

impl Layer for ToolEventLayer {
    type Send = ToolEvent;
    type Received = ToolEvent;

    type Sender = BoxSink<'static, Self::Send, anyhow::Error>;
    type Receiver = BoxStream<'static, Self::Received>;

    fn split(self) -> (Self::Sender, Self::Receiver) {
        (self.0, self.1)
    }
}

impl<L> LayerChain<L, ()> for ToolEventLayer where L: Layer<Send = PubSubMessage, Received = PubSubMessage> {
    fn chain(layer: L, _: ()) -> Self {
        let (tx, rx) = layer.split();

        let rx = rx.filter_map(|msg| {
            if WorkerEvent::is(&msg) 
                && let Ok(event) = WorkerEvent::try_from(msg.clone()) 
                && let WorkerEvent::JobDone{id, result} = event
            
            {
                return std::future::ready(Some(ToolEvent::JobDone{id, result})).boxed();
            }

           std::future::ready(ToolEvent::try_from(msg).ok()).boxed()
        }).boxed();

        let tx = tx.with(|event: ToolEvent| {
            std::future::ready(Ok(PubSubMessage::from(event)))
        }).boxed_sink();

        Self(tx, rx)
    }
}