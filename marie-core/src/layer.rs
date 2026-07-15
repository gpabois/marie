use futures::{Sink, Stream};

pub trait Layer<Error=anyhow::Error> {
    type Send;
    type Received;
    type Sender: Sink<Self::Send, Error=Error> + Send + 'static;
    type Receiver: Stream<Item=Self::Received> + Send  + 'static;

    fn split(self) -> (Self::Sender, Self::Receiver);
}

pub trait IntoService<S> {
    type Args;
    
    fn into_service(self, args: Self::Args) -> S;
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