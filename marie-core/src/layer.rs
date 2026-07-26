use futures::{Sink, Stream, StreamExt, stream::BoxStream};

use crate::sink::{BoxSink, SinkBoxExt};

pub struct BoxLayer<S, R, E=crate::Error>(BoxSink<'static, S, E>, BoxStream<'static, R>);

impl<S, R, E> BoxLayer<S, R, E> {
    pub fn new(
        tx: impl Sink<S, Error=E> + Send + 'static,
        rx: impl Stream<Item=R> + Send + 'static
    ) -> Self {
        Self(tx.boxed_sink(), rx.boxed())
    }
}

impl<S, R, E> Layer<E> for BoxLayer<S, R, E> 
    where S: Send  + 'static, R: Send  +'static, E: 'static
{
    type Send = S;
    type Received = R;

    type Sender = BoxSink<'static, S, E>;
    type Receiver = BoxStream<'static, R>;

    fn split(self) -> (Self::Sender, Self::Receiver) {
        (self.0, self.1)
    }
}

pub trait Layer<Error=crate::Error>: Sized {
    type Send;
    type Received;
    type Sender: Sink<Self::Send, Error=Error> + Send + 'static;
    type Receiver: Stream<Item=Self::Received> + Send + 'static;

    fn split(self) -> (Self::Sender, Self::Receiver);

    fn boxed(self) -> BoxLayer<Self::Send, Self::Received, Error> {
        let (tx, rx) = self.split();
        BoxLayer::new(tx, rx)
    }

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

