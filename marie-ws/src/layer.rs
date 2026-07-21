use futures::{Sink, SinkExt as _, Stream, StreamExt as _, stream::BoxStream};
use marie_core::{layer::{Layer, LayerChain}, sink::{BoxSink, SinkBoxExt as _}};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::broadcast;
use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};
use tracing::warn;

use crate::protocol::{Muxed, RawMessage};

/// Moitié `Sink` d'un [`WsLayer`] — un `broadcast::Sender` plutôt qu'un canal
/// mono-consommateur car plusieurs [`WsMux`] (un par canal muxé, voir
/// [`Muxed`]) partagent la même connexion websocket physique.
#[derive(Clone)]
pub struct WsRawSender(broadcast::Sender<RawMessage>);

impl Sink<RawMessage> for WsRawSender {
    type Error = anyhow::Error;

    fn poll_ready(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: RawMessage) -> Result<(), Self::Error> {
        self.0.send(item)?;
        Ok(())
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_close(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}

/// Moitié `Stream` d'un [`WsLayer`] — voir [`WsRawSender`] pour la raison du
/// `broadcast`. `Lagged` (abonné trop en retard) est absorbé silencieusement,
/// comme `network::actor::NetworkReceiver`.
pub struct WsRawReceiver(BroadcastStream<RawMessage>);

impl Stream for WsRawReceiver {
    type Item = RawMessage;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        loop {
            return match std::pin::Pin::new(&mut self.0).poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(msg))) => std::task::Poll::Ready(Some(msg)),
                std::task::Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(skipped)))) => {
                    warn!(skipped, "abonné websocket en retard, messages perdus");
                    continue;
                }
                std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
                std::task::Poll::Pending => std::task::Poll::Pending,
            };
        }
    }
}

/// Transport brut d'une connexion websocket : un `broadcast::Sender`/`Receiver`
/// de [`RawMessage`], partagé entre plusieurs [`WsMux`] muxant chacun un canal
/// applicatif différent sur la même socket physique.
pub struct WsLayer(WsRawSender, WsRawReceiver);

impl WsLayer {
    pub fn new(tx: broadcast::Sender<RawMessage>, rx: broadcast::Receiver<RawMessage>) -> Self {
        Self(WsRawSender(tx), WsRawReceiver(BroadcastStream::new(rx)))
    }
}

impl Layer for WsLayer {
    type Send = RawMessage;
    type Received = RawMessage;

    type Sender = WsRawSender;
    type Receiver = WsRawReceiver;

    fn split(self) -> (Self::Sender, Self::Receiver) {
        (self.0, self.1)
    }
}

/// Un canal applicatif muxé sur un [`WsLayer`] partagé : les messages sortants
/// sont enveloppés dans un [`Muxed`] taggé `channel`, les messages entrants
/// sont filtrés pour ne garder que ceux du même `channel`.
pub struct WsMux<S, R>(BoxSink<'static, S, anyhow::Error>, BoxStream<'static, R>);

impl<S, R> Layer for WsMux<S, R>
where
    S: Serialize + Send + 'static,
    R: DeserializeOwned + Send + 'static,
{
    type Send = S;
    type Received = R;

    type Sender = BoxSink<'static, S, anyhow::Error>;
    type Receiver = BoxStream<'static, R>;

    fn split(self) -> (Self::Sender, Self::Receiver) {
        (self.0, self.1)
    }
}

impl<S, R> LayerChain<WsLayer, String> for WsMux<S, R>
where
    S: Serialize + Send + 'static,
    R: DeserializeOwned + Send + 'static,
{
    fn chain(layer: WsLayer, channel: String) -> Self {
        Self::new(layer, channel)
    }
}

impl<S, R> WsMux<S, R>
where
    S: Serialize + Send + 'static,
    R: DeserializeOwned + Send + 'static,
{
    pub fn new(layer: WsLayer, channel: String) -> Self {
        let (tx, rx) = layer.split();

        let out_channel = channel.clone();
        let tx = tx
            .with(move |msg: S| {
                let out_channel = out_channel.clone();
                async move {
                    let payload = serde_json::to_vec(&msg)?;
                    let muxed = Muxed { channel: out_channel, payload };
                    anyhow::Ok(RawMessage::Bytes(serde_json::to_vec(&muxed)?))
                }
            })
            .boxed_sink();

        let rx = rx
            .filter_map(move |raw| {
                let channel = channel.clone();
                std::future::ready((|| {
                    let RawMessage::Bytes(bytes) = raw else { return None };
                    let muxed: Muxed = serde_json::from_slice(&bytes).ok()?;
                    if muxed.channel != channel {
                        return None;
                    }
                    serde_json::from_slice(&muxed.payload).ok()
                })())
            })
            .boxed();

        Self(tx, rx)
    }
}
