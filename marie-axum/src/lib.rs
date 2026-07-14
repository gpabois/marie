//! Passerelle Marie exposée en HTTP/WebSocket via `axum`, destinée à un
//! client tiers (`marie-leptos` ou tout autre, voir sa doc de crate) qui n'a
//! pas vocation à parler `libp2p` directement.
//!
//! Ce crate est une boîte à outils, pas un serveur clé en main : il ne
//! prescrit ni authentification, ni protocole applicatif figé au-delà du
//! vocabulaire minimal défini dans [`protocol`]. Deux besoins motivent ce
//! choix (voir la demande d'origine) :
//!
//! - **Sécurité au choix de l'appelant** — [`gateway::MarieGateway`] ne fait
//!   aucune hypothèse sur l'identité de qui se connecte : à l'appelant de
//!   placer son propre extracteur/middleware `axum` (session cookie, JWT,
//!   mTLS, ...) devant les routes qui l'utilisent. [`ws::router`] fournit un
//!   point de départ *sans* authentification, à ne monter que derrière sa
//!   propre couche de sécurité (ou à ignorer complètement, voir plus bas).
//! - **Websocket composable** — [`ws::events`]/[`ws::dispatch`] sont les
//!   briques utilisées par [`ws::serve`] (la boucle "batteries incluses"),
//!   mais restent utilisables séparément : un appelant qui veut mélanger le
//!   vocabulaire Marie ([`protocol::ClientMessage`]/[`protocol::ServerMessage`])
//!   avec ses propres messages sur le même socket écrit sa propre boucle
//!   `tokio::select!` en s'appuyant dessus, plutôt que de dépendre de
//!   [`ws::serve`] (voir sa doc pour ce qu'elle ignore).
pub mod gateway;
pub mod protocol;
pub mod ws;

pub use gateway::MarieGateway;
pub use protocol::{ClientMessage, ServerMessage};
