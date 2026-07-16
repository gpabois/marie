use futures::{SinkExt as _, StreamExt as _, stream::BoxStream};

use crate::{job::Job, layer::{IntoService, Layer, LayerChain}, network::worker::{WorkerEvent, server::{WorkerServer, WorkerServerActor, WorkerServerArgs}}, pubsub::PubSubMessage, sink::{BoxSink, SinkBoxExt as _}};

pub struct WorkerEventLayer(<Self as Layer>::Sender, <Self as Layer>::Receiver);

impl Layer for WorkerEventLayer {
    type Send = WorkerEvent;
    type Received = WorkerEvent;
    type Sender = BoxSink<'static, Self::Send, anyhow::Error>;
    type Receiver = BoxStream<'static, Self::Received>;

    fn split(self) -> (Self::Sender, Self::Receiver) {
        (self.0, self.1)
    }
}


impl<B, Cx, T> IntoService<WorkerServer<Cx>, WorkerServerArgs<Cx, B>> for T 
    where 
            T: Layer<Send = WorkerEvent, Received = WorkerEvent>,
            B: Fn(&Job) -> Cx + Send + Sync + 'static,
            Cx: Send + 'static
{

    fn into_service(self, args: WorkerServerArgs<Cx, B>) -> WorkerServer<Cx> {
        WorkerServerActor::new(self, args)
    }
}

impl<L> LayerChain<L, ()> for WorkerEventLayer where L: Layer<Send=PubSubMessage, Received=PubSubMessage> {
    
    fn chain(layer: L, _: ()) -> Self {
        let (tx, rx) = layer.split();

        let tx = tx.with(|event: WorkerEvent| {
            std::future::ready(Ok(PubSubMessage {
                id: String::default(),
                source: None,
                topic: event.topic(),
                payload: serde_json::to_vec(&event).unwrap()
            }))
        }).boxed_sink();

        let rx = rx.filter_map(|msg| {
            if msg.topic.starts_with(WorkerEvent::TOPIC_PREFIX) {
                let event: WorkerEvent = serde_json::from_slice(&msg.payload).unwrap();
                std::future::ready(Some(event))
            } else {
                std::future::ready(None)
            }
        }).boxed();

        Self(tx, rx)
    }
}