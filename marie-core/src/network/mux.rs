use futures::{SinkExt as _, StreamExt, stream::{self, BoxStream}};
use libp2p::PeerId;
use serde::{Deserialize, Serialize};

use crate::{
    layer::{Layer, LayerChain}, 
    network::protocol::{NetworkCommand, NetworkEvent}, 
    sink::{BoxSink, SinkBoxExt as _}
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
    pub channel: String,
    pub destination: Option<PeerId>,
    pub source: Option<PeerId>,
    pub payload: Vec<u8>
}

pub struct FrameLayer(BoxSink<'static, Frame, anyhow::Error>, BoxStream<'static, Frame>);

impl Layer for FrameLayer {
    type Send = Frame;
    type Received = Frame;
    type Sender = BoxSink<'static, Frame, anyhow::Error>;
    type Receiver = BoxStream<'static, Frame>;

    fn split(self) -> (Self::Sender, Self::Receiver) {
        (self.0, self.1)
    }
}

impl<T> LayerChain<T, ()> for FrameLayer where T: Layer<Send=NetworkCommand, Received=NetworkEvent> {
    fn chain(layer: T, _: ()) -> Self {
        Self::new(layer)
    }
}

impl FrameLayer {
    pub fn new(layer: impl Layer<Send=NetworkCommand, Received=NetworkEvent>) -> Self {
        let (tx, rx) = layer.split();
        
        let tx = tx.with_flat_map(|frame: Frame| {
            stream::iter(vec![Ok(NetworkCommand::SendFrame(frame))])
        }).boxed_sink();

        let rx = rx.filter_map(|event| {
            std::future::ready(match event {
                NetworkEvent::ReceivedFrame(frame) => Some(frame),
                _ => None
            })
        }).boxed();

        Self(tx, rx)
    }
}