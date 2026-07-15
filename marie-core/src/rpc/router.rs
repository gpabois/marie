use std::{collections::HashMap, sync::Arc, sync::Mutex};

use futures::{SinkExt, StreamExt, stream::BoxStream};
use serde::Serialize;

use crate::{layer::{Layer, LayerChain}, rpc::RpcMessage, sink::{BoxSink, SinkBoxExt}};

#[derive(Default, Clone)]
pub struct RpcRelayService(Arc<Mutex<HashMap::<String, serde_json::Value>>>);

impl RpcRelayService {
    pub fn relay_on_recv(&self, name: impl ToString, destination: impl Serialize) {
        self.0.lock().unwrap().insert(name.to_string(), serde_json::to_value(destination).unwrap());
    }
    
    fn should_be_relayed(&self, name: &str) -> Option<serde_json::Value> {
        self.0.lock().unwrap().get(name).cloned() 
    }
}

/// Couche permettant de forwarder automatiquement des RPC
pub struct RpcRelayLayer(BoxSink<'static, RpcMessage, anyhow::Error>, BoxStream<'static, RpcMessage>);

impl Layer for RpcRelayLayer {
    type Send = RpcMessage;
    type Received = RpcMessage;
    type Sender = BoxSink<'static, RpcMessage, anyhow::Error>;
    type Receiver = BoxStream<'static, RpcMessage>;

    fn split(self) -> (Self::Sender, Self::Receiver) {
        (self.0, self.1)
    }
}

impl<P, F> LayerChain<P, (RpcRelayService, F)> for RpcRelayLayer where P: Layer<Send=RpcMessage, Received=RpcMessage>, F: Layer<Send=RpcMessage, Received=RpcMessage>
{
    fn chain(layer: P, args: (RpcRelayService, F)) -> Self {
        Self::new(layer, args.0, args.1)
    }
}
impl RpcRelayLayer {
    pub fn new(
            layer: impl Layer<Send=RpcMessage, Received=RpcMessage>, 
            svc: RpcRelayService, 
            forward: impl Layer<Send=RpcMessage, Received=RpcMessage>
    ) -> Self {
        use RpcMessage::Call;

        let (tx, rx) = layer.split();
        let (forward_tx, _) = forward.split();
        let mut forward_tx = forward_tx.boxed_sink();
        
        let rx = rx.filter_map(move |mut msg| {
            match &msg {
                Call(call) => {
                    if let Some(forward_to) = svc.should_be_relayed(&call.name) {
                        msg.set_destination(Some(forward_to));
                        let _ = forward_tx.send(msg);
                        return std::future::ready(None);
                    }
                    std::future::ready(Some(msg))
                },
                _ => std::future::ready(Some(msg))
            }
        }).boxed();

        let tx = tx.boxed_sink();
        
        Self(tx, rx)
    }
}