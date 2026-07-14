//! Binaire serveur de `marie-web` (feature `ssr`) — démarre son propre
//! cluster Marie local (control plane + worker + persistency, un seul nœud
//! de chaque, voir [`start_embedded_cluster`]) puis s'y connecte comme
//! client (voir `marie_axum::gateway::MarieGateway::connect`) et rend
//! l'interface web au-dessus via `leptos_axum` (remplace le `ServeDir`/
//! `index.html` statique de l'ancien `marie-axum-leptos` par un rendu
//! serveur réel, avec hydratation côté client).
//!
//! Cluster à un seul nœud par rôle, embarqué dans ce même process : adapté à
//! un usage dev/démo en une seule commande, pas à un déploiement en
//! production (qui voudrait plusieurs workers/control planes répartis sur
//! plusieurs machines — voir l'exemple du README racine pour ce cas, qui
//! démarre les rôles dans des process séparés partageant le même
//! `master_key`).

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context as _;
use axum::Router;
use axum::routing::put;
use clap::Parser;
use leptos::prelude::*;
use leptos_axum::{LeptosRoutes, generate_route_list};
use marie_axum::gateway::MarieGateway;
use marie_axum::ws::GatewayState;
use marie_core::mode::executable::RustRegistry;
use marie_core::network::cp::log_store::redb_backend::RedbLogBackend;
use marie_core::persistency::RedbStore;
use marie_core::persistency::filesystem::FilesystemConfig;
use marie_core::persistency::postgres::run_migrations;
use marie_core::secret::SecretKey;
use marie_core::{Marie, MarieConfig, MarieHandle, NodeRole};
use marie_web::app::{App, shell};
use marie_web::hitl_upload;
use object_store::ObjectStore;
use sqlx::postgres::PgPool;
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
struct Args {
    /// Secret de cluster partagé (voir `marie_core::secret::SecretManager`),
    /// en hexadécimal (64 caractères pour 32 octets) — utilisé à la fois pour
    /// le cluster local démarré par ce binaire (voir
    /// [`start_embedded_cluster`]) et pour son client gateway ; doit être
    /// identique à celui de tout autre nœud Marie sur le même réseau.
    #[arg(long, env = "MARIE_MASTER_KEY")]
    master_key: String,

    /// Chaîne de connexion PostgreSQL — porte l'arborescence `/files` du VFS
    /// de session (inodes/alias, voir `persistency::postgres::run_migrations`),
    /// pas le contenu lui-même (voir `FilesystemConfig::S3`).
    #[arg(long, env = "DATABASE_URL", default_value = "postgres://localhost/marie")]
    database_url: String,
}

fn parse_master_key(raw: &str) -> anyhow::Result<SecretKey> {
    let bytes = hex::decode(raw).context("--master-key doit être de l'hexadécimal")?;
    let len = bytes.len();
    bytes.try_into().map_err(|_| anyhow::anyhow!("--master-key doit décoder en 32 octets (reçu {len})"))
}

/// Poignées des trois nœuds démarrés par [`start_embedded_cluster`] — à
/// arrêter proprement (voir [`MarieHandle::shutdown`]) dans l'ordre
/// worker → persistency → control plane (même ordre que l'exemple du README
/// racine : laisser le worker/persistency terminer leur travail en vol avant
/// de couper le control plane dont ils dépendent encore pour ça).
struct EmbeddedCluster {
    control_plane: MarieHandle,
    worker: MarieHandle,
    persistency: MarieHandle,
}

impl EmbeddedCluster {
    async fn shutdown(self) {
        self.worker.shutdown().await;
        self.persistency.shutdown().await;
        self.control_plane.shutdown().await;
    }
}

/// Démarre un control plane, un worker et un nœud de persistency, un seul de
/// chaque, dans ce même process — voir la doc de module sur le périmètre visé
/// (dev/démo) et l'exemple du README racine dont ce code reprend le
/// déroulé. `pool`/`object_store` sont partagés avec le worker/persistency
/// (VFS `/files` commun au cluster) ; ce sont les mêmes que ceux utilisés
/// ensuite par le client gateway de `main`.
///
/// Les stores `redb` (catalogues du control plane, log Raft, sessions CRDT
/// de persistency) vivent sous un répertoire temporaire propre à ce run
/// (`std::process::id()`) : l'état ne survit pas à un redémarrage, ce qui
/// convient à l'usage dev/démo visé ici (voir `FilesystemConfig::Memory`
/// juste au-dessus dans `main`, même logique de jetable).
async fn start_embedded_cluster(master_key: SecretKey, pool: PgPool, object_store: Arc<dyn ObjectStore>) -> anyhow::Result<EmbeddedCluster> {
    let cluster_dir = std::env::temp_dir().join(format!("marie-web-cluster-{}", std::process::id()));
    std::fs::create_dir_all(&cluster_dir).context("création du répertoire de stockage du cluster embarqué")?;

    let control_plane = Marie::new(MarieConfig::builder().master_key(master_key).build());
    let catalogs_store = Arc::new(RedbStore::open(cluster_dir.join("catalogs.redb")).context("ouverture du store de catalogues")?);
    let raft_log_backend = Arc::new(RedbLogBackend::open(cluster_dir.join("raft-log.redb")).context("ouverture du log Raft")?);
    let control_plane_handle = control_plane.start(NodeRole::ControlPlane {
        raft_log_backend,
        model_store: catalogs_store.clone(),
        tool_store: catalogs_store.clone(),
        expert_store: catalogs_store.clone(),
        state_graph_store: catalogs_store,
    });

    let worker = Marie::new(MarieConfig::builder().master_key(master_key).build());
    let worker_handle = worker.start(NodeRole::Worker { pool: pool.clone(), store: object_store.clone(), rust_registry: RustRegistry::new() });

    let persistency = Marie::new(MarieConfig::builder().master_key(master_key).build());
    let session_store = Arc::new(RedbStore::open(cluster_dir.join("sessions.redb")).context("ouverture du store de sessions")?);
    let persistency_handle =
        persistency.start(NodeRole::Persistency { store: session_store.clone(), workspace_store: session_store, pool, object_store });

    // Laisse le temps à mDNS de découvrir les pairs et au cluster Raft de
    // s'initialiser (voir `network::cp::BOOTSTRAP_DELAY`, 3s) avant que le
    // client gateway de `main` ne s'y connecte.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    Ok(EmbeddedCluster { control_plane: control_plane_handle, worker: worker_handle, persistency: persistency_handle })
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv()?;

    tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let args = Args::parse();
    let master_key = parse_master_key(&args.master_key)?;

    let pool = PgPoolOptions::new().connect(&args.database_url).await.context("connexion PostgreSQL")?;
    run_migrations(&pool).await?;
    // Contenu des fichiers de session en mémoire pour cet exemple — voir
    // `FilesystemConfig::S3` pour un backend durable (S3/compatible S3).
    let object_store = FilesystemConfig::Memory.build().context("initialisation du stockage de fichiers")?;

    tracing::info!("démarrage du cluster Marie embarqué (control plane + worker + persistency)");
    let cluster = start_embedded_cluster(master_key, pool.clone(), object_store.clone())
        .await
        .context("démarrage du cluster Marie embarqué")?;

    let (gateway, gateway_handle) = MarieGateway::connect(master_key).await.context("connexion au cluster Marie")?;
    let sessions = gateway.session_client(pool, object_store).context("client de session")?;
    let gateway_state = GatewayState { gateway, sessions };

    let conf = get_configuration(None)?;
    let leptos_options = conf.leptos_options;
    let addr: SocketAddr = leptos_options.site_addr;
    let routes = generate_route_list(App);

    let app = Router::<LeptosOptions>::new()
        .leptos_routes_with_context(&leptos_options, routes, {
            let gateway_state = gateway_state.clone();
            move || provide_context(gateway_state.clone())
        }, {
            // `app_fn` doit produire le document complet (`<!DOCTYPE html>` +
            // `<HydrationScripts>`, voir `app::shell`) — `leptos_axum` ne
            // l'enveloppe pas lui-même. Passer `App` nu ici (comme avant)
            // saute tout ça : le bundle wasm n'est alors jamais chargé, donc
            // aucun `on:click` ne s'attache jamais après coup, sur aucun
            // bouton de l'app.
            let leptos_options = leptos_options.clone();
            move || shell(leptos_options.clone())
        })
        .fallback(leptos_axum::file_and_error_handler(shell))
        .with_state(leptos_options)
        // Téléversement HITL (contenu binaire brut) — reste une route `axum`
        // classique sur son propre état, voir la doc de `hitl_upload` sur
        // pourquoi une fonction `#[server]` ne convient pas ici.
        .merge(
            Router::new()
                .route("/api/sessions/{session_id}/hitl-uploads/{*path}", put(hitl_upload::write_binary_file))
                .with_state(gateway_state.clone()),
        )
        // La websocket (`/ws`, voir `marie_axum::ws::router`) est un flux
        // bidirectionnel de longue durée — hors du périmètre de
        // `leptos_routes_with_context` (routes/fonctions serveur, toutes en
        // requête/réponse) : montée telle quelle, comme documenté dans
        // `marie-axum` (routes agnostiques du framework de routage appelant).
        .merge(marie_axum::ws::router(gateway_state));

    tracing::info!(%addr, "serveur marie-web démarré");
    let listener = tokio::net::TcpListener::bind(&addr).await.context("écoute HTTP")?;
    axum::serve(listener, app.into_make_service()).with_graceful_shutdown(shutdown_signal()).await?;

    // Arrêt propre, dans l'ordre inverse du démarrage : le client gateway
    // d'abord (ne dépend de rien d'autre ici), puis le cluster embarqué
    // (voir `EmbeddedCluster::shutdown` sur l'ordre worker → persistency →
    // control plane).
    gateway_handle.shutdown().await;
    cluster.shutdown().await;

    Ok(())
}
