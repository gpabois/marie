use futures::{Sink, Stream, StreamExt, stream::BoxStream};

use crate::sink::{BoxSink, SinkBoxExt};

pub trait Layer<Error=anyhow::Error>: Sized {
    type Send;
    type Received;
    type Sender: Sink<Self::Send, Error=Error> + Send + 'static;
    type Receiver: Stream<Item=Self::Received> + Send  + 'static;

    fn split(self) -> (Self::Sender, Self::Receiver);

    fn boxed_split(self) -> (BoxSink<'static, Self::Send, Error>, BoxStream<'static, Self::Received>) {
        let (tx, rx) = self.split();
        (tx.boxed_sink(), rx.boxed())
    }
}

pub trait IntoService<S, Args> {   
    fn into_service(self, args: Args) -> S;
}

pub trait LayerChain<L, Args> {
    fn chain(layer: L, args: Args) -> Self;
}

pub trait LayerExt {
    fn chain<T, Args>(self, args: Args) -> T where T: LayerChain<Self, Args>, Self: std::marker::Sized {
        T::chain(self, args)
    }
}

impl<T> LayerExt for T where T: Layer {}