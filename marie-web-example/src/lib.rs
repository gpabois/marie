//! `marie-web` — migration de l'ancien `marie-axum-leptos` (CSR pur + gateway
//! axum fait main) vers un rendu Leptos SSR + hydratation standard
//! (`leptos_axum`), compilé deux fois (jamais ensemble) via deux features
//! Cargo :
//! - `hydrate` → `wasm32-unknown-unknown`, le bundle client qui hydrate le
//!   HTML rendu côté serveur (voir `hydrate()` ci-dessous, appelé au chargement
//!   de la page).
//! - `ssr`, natif → le binaire serveur (`src/main.rs`) qui rend l'app via
//!   `leptos_axum` puis sert ce bundle.
//!
//! `app`/`components`/`dto` compilent sous les deux features sans
//! distinction : voir la doc de `state_graph_editor`/`ws_client` pour les
//! quelques points qui restent spécifiquement client-only.

// L'arbre de vues de `ConfigPanel` (4 sections composées, chacune avec ses
// `<For>`) imbrique suffisamment de types concrets `tachys` distincts pour
// dépasser la limite de récursion par défaut du compilateur lors du calcul
// de layout (uniquement en codegen complet — `cargo build`/`leptos`, pas
// `cargo check`, qui saute cette étape).
#![recursion_limit = "512"]

pub mod api;
pub mod app;
pub mod components;
pub mod dto;
pub mod hitl_upload;
pub mod ws_client;

pub use app::App;

#[cfg(feature = "hydrate")]
#[wasm_bindgen::prelude::wasm_bindgen(start)]
pub fn hydrate() {
    console_error_panic_hook::set_once();
    leptos::mount::hydrate_body(App);
}
