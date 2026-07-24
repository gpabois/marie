use futures::{Sink, SinkExt, Stream, StreamExt, stream::BoxStream};

use crate::{layer::{Layer, LayerChain}, network::{swarm::{NetworkReceiver, NetworkSender}, mux::Frame, protocol::{NetworkCommand::SendFrame, NetworkEvent}}, rpc::RpcMessage, sink::{BoxSink, SinkBoxExt}};


/// Multiplexer de RPC sur le cluster Marie des appels RPC
pub struct RpcMuxLayer(BoxSink<'static, RpcMessage, anyhow::Error>, BoxStream<'static, RpcMessage>);

impl Layer for RpcMuxLayer {
    type Send = RpcMessage;
    type Received = RpcMessage;
    type Receiver = BoxStream<'static, RpcMessage>;
    type Sender = BoxSink<'static, RpcMessage, anyhow::Error>;

    fn split(self) -> (Self::Sender, Self::Receiver) {
        (self.0, self.1)
    }
}


impl<T> LayerChain<T, ()> for RpcMuxLayer where T: Layer<Received = Frame, Send = Frame> {

    fn chain(layer: T, _: ()) -> Self {
        Self::new(layer)
    }
}

impl RpcMuxLayer {
    pub fn new(layer: impl Layer<Received = Frame, Send = Frame>) -> Self {
        let (tx, rx) = layer.split();

        let rx = rx.filter_map(|frame| {
            let Ok(mut msg) = serde_json::from_slice::<RpcMessage>(&frame.payload) else { return std::future::ready(None); };
            if msg.source().is_none() { msg.set_source(frame.source); }
            if msg.destination().is_none() { msg.set_destination(frame.destination); }
            std::future::ready(Some(msg))
        }).boxed();

        let tx = tx.with(|item: RpcMessage| {
            std::future::ready::<Result<Frame, anyhow::Error>>(Ok(Frame {
                channel: "rpc".to_string(),
                destination: item.destination(),
                source: item.source(),
                payload: serde_json::to_vec(&item).unwrap(),
            }))
        }).boxed_sink();
        
        RpcMuxLayer(tx, rx)
    }

}
