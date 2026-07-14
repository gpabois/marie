//! Téléversement d'un fichier en réponse à un formulaire HITL (voir
//! `crate::dto::hitl_upload_path`, `marie_core::hitl::QuestionKind::FileUpload`)
//! — sur le même modèle que `crate::api::write_file`, mais un corps d'octets
//! bruts plutôt que du JSON : un fichier téléversé (PDF, image, ...) n'a
//! aucune raison d'être du texte UTF-8, contrairement au cas d'usage du
//! navigateur de fichiers de session (voir `crate::dto::FileContentDto`).
//!
//! Reste une route `axum` classique montée à la main dans `main.rs`, plutôt
//! qu'une fonction `#[server]` comme le reste de `crate::api` : les fonctions
//! serveur Leptos attendent des arguments (dé)sérialisables par le codec
//! configuré, pas un corps de requête binaire arbitraire.

#[cfg(feature = "ssr")]
pub async fn write_binary_file(
    axum::extract::State(state): axum::extract::State<marie_axum::ws::GatewayState>,
    axum::extract::Path((session_id, path)): axum::extract::Path<(String, String)>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    let session_id = match session_id.parse::<marie_core::id::ID>() {
        Ok(id) => id,
        Err(error) => return (StatusCode::BAD_REQUEST, format!("identifiant invalide '{session_id}' : {error}")).into_response(),
    };
    match state.sessions.write_file(session_id, &path, body.to_vec()).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(error) => (StatusCode::BAD_GATEWAY, error.to_string()).into_response(),
    }
}

/// Écrit un contenu binaire brut à `path` (voir `crate::dto::hitl_upload_path`)
/// — distinct de [`crate::api::write_file`], qui ne sait écrire que du texte
/// UTF-8. Appelée depuis `components::chat_view` (composant partagé, voir sa
/// doc de module) mais toujours depuis un `on:change` — jamais lors du rendu
/// serveur — donc seule la variante `hydrate` a un corps réel ; la variante
/// `ssr` n'existe que pour que ce site d'appel compile côté serveur.
#[cfg(feature = "hydrate")]
pub async fn write_hitl_upload(session_id: &str, path: &str, bytes: &[u8]) -> Result<(), String> {
    let array = js_sys::Uint8Array::from(bytes);
    let response = gloo_net::http::Request::put(&format!("/api/sessions/{session_id}/hitl-uploads/{path}"))
        .body(array)
        .map_err(|error| error.to_string())?
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if !response.ok() {
        return Err(response.text().await.unwrap_or_else(|_| format!("HTTP {}", response.status())));
    }
    Ok(())
}

#[cfg(not(feature = "hydrate"))]
pub async fn write_hitl_upload(_session_id: &str, _path: &str, _bytes: &[u8]) -> Result<(), String> {
    unreachable!("write_hitl_upload n'est appelée que depuis un event handler côté client, jamais lors du rendu serveur")
}
