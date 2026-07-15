use std::pin::Pin;

use futures::Sink;


pub type BoxSink<'a, Item, Error> = Pin<Box<dyn Sink<Item, Error = Error> + Send + 'a>>;

pub trait SinkBoxExt<Item, Error>: Sink<Item, Error = Error> {
    fn boxed_sink<'a>(self) -> Pin<Box<dyn Sink<Item, Error = Error> + Send + 'a>>
    where
        Self: Sized + Send + 'a,
    {
        Box::pin(self)
    }
}

impl<S, Item, Error> SinkBoxExt<Item, Error> for S where S: Sink<Item, Error = Error> {}