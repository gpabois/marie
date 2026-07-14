//! Connexion websocket vers `/ws` (voir `marie_axum::ws::router`, monté tel
//! quel par `main.rs`) — un flux bidirectionnel de longue durée n'est pas ce
//! qu'une fonction `#[server]` (voir `crate::api`) sait exprimer, donc ce
//! module reste, comme l'ancien `crate::api::SessionSocket`, un client
//! `gloo-net` fait main.
//!
//! Référencé depuis `components::chat_view` (composant partagé ssr/hydrate,
//! voir sa doc de module) mais n'ouvre jamais réellement de connexion que
//! côté client (dans un `Effect`, inerte côté serveur en Leptos 0.8) — la
//! variante `ssr` de [`SessionSocket`] ci-dessous n'existe que pour que ce
//! site d'appel compile côté serveur, elle ne fait jamais rien en pratique.

use crate::dto::{ClientMessageDto, ServerMessageDto};

#[cfg(feature = "hydrate")]
pub use hydrate::SessionSocket;

#[cfg(feature = "hydrate")]
mod hydrate {
    use futures::stream::SplitSink;
    use gloo_net::websocket::Message;
    use gloo_net::websocket::futures::WebSocket;

    use super::{ClientMessageDto, ServerMessageDto};

    fn ws_url() -> String {
        let location = web_sys::window().expect("le frontend s'exécute toujours dans une fenêtre navigateur").location();
        let is_https = location.protocol().unwrap_or_default() == "https:";
        let host = location.host().unwrap_or_default();
        format!("{}://{host}/ws", if is_https { "wss" } else { "ws" })
    }

    /// `on_message` est appelé pour chaque [`ServerMessageDto`] reçu, depuis
    /// une tâche `wasm-bindgen-futures` dédiée (voir
    /// [`wasm_bindgen_futures::spawn_local`]) ; un message qui ne désérialise
    /// pas est ignoré silencieusement, comme le fait déjà
    /// `marie_axum::ws::serve` côté serveur pour les messages entrants.
    pub struct SessionSocket {
        sink: SplitSink<WebSocket, Message>,
    }

    impl SessionSocket {
        pub fn connect(on_message: impl Fn(ServerMessageDto) + 'static) -> Result<Self, String> {
            use futures::StreamExt as _;

            let ws = WebSocket::open(&ws_url()).map_err(|error| error.to_string())?;
            let (sink, mut stream) = ws.split();

            wasm_bindgen_futures::spawn_local(async move {
                while let Some(Ok(message)) = stream.next().await {
                    let Message::Text(text) = message else { continue };
                    if let Ok(parsed) = serde_json::from_str::<ServerMessageDto>(&text) {
                        on_message(parsed);
                    }
                }
            });

            Ok(Self { sink })
        }

        pub async fn send(&mut self, message: &ClientMessageDto) -> Result<(), String> {
            use futures::SinkExt as _;

            let text = serde_json::to_string(message).map_err(|error| error.to_string())?;
            self.sink.send(Message::Text(text)).await.map_err(|error| error.to_string())
        }
    }
}

#[cfg(not(feature = "hydrate"))]
pub struct SessionSocket {
    _private: (),
}

#[cfg(not(feature = "hydrate"))]
impl SessionSocket {
    pub fn connect(_on_message: impl Fn(ServerMessageDto) + 'static) -> Result<Self, String> {
        unreachable!("SessionSocket n'est ouvert que côté client, jamais lors du rendu serveur — voir la doc de module")
    }

    pub async fn send(&mut self, _message: &ClientMessageDto) -> Result<(), String> {
        unreachable!("SessionSocket n'est ouvert que côté client, jamais lors du rendu serveur — voir la doc de module")
    }
}
