use futures::{Sink, SinkExt, Stream, StreamExt, stream::BoxStream};

use crate::{layer::{Layer, LayerChain}, network::{mux::Frame, actor::{NetworkCommand::SendFrame, NetworkEvent, NetworkReceiver, NetworkSender}}, rpc::RpcMessage , sink::{BoxSink, SinkBoxExt}};


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

pub struct RpcReceiver(NetworkReceiver);

impl Stream for RpcReceiver {
    type Item = RpcMessage;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        match self.0.poll_next_unpin(cx) {
            std::task::Poll::Ready(Some(NetworkEvent::ReceivedFrame(frame))) if frame.channel.as_str() == "rpc" => {
                let mut msg: RpcMessage = serde_json::from_slice(&frame.payload).unwrap();
                if msg.source().is_none() { msg.set_source(frame.source); }
                std::task::Poll::Ready(Some(msg))
            },
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            _ => std::task::Poll::Pending,
        }
    }
    
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

pub struct RpcSender(NetworkSender);

impl Sink<RpcMessage> for RpcSender {
    type Error = anyhow::Error;

    fn poll_ready(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.0.poll_ready_unpin(cx)
    }

    fn start_send(mut self: std::pin::Pin<&mut Self>, item: RpcMessage) -> Result<(), Self::Error> {
        let frame = Frame {
            channel: "rpc/request".to_string(),
            destination: item.destination(),
            source: None,
            payload: serde_json::to_vec(&item)?,
        };

        let cmd = SendFrame(frame);

        self.0.start_send_unpin(cmd)?;

        Ok(())
    }

    fn poll_flush(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.0.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.0.poll_close_unpin(cx)
    }
}