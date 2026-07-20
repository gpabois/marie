use libp2p::rendezvous::Namespace;
use typed_builder::TypedBuilder;

use crate::{
    expert::{NS_EXPERT, server::ExpertServer},
    layer::LayerExt as _,
    model::{NS_MODEL, server::ModelServer},
    network::{
        actor::NetworkActor,
        bootstrap::{self, client::BootstrapArgs},
        create_swarm,
        worker::{self, client::WorkerClientArgs, layers::WorkerEventLayer},
    },
    pubsub::layers::PubSubLayer,
    rpc,
    secret::SecretManager,
    session::{self, server::SessionServerArgs, store::SessionStoreActor},
    store::{PgStore, run_migrations},
    tools::{NS_TOOL, layers::ToolEventLayer, server::{ToolServer, ToolServerActor}},
    workspace::{self, server::WorkspaceServerArgs, store::WorkspaceStoreActor},
};

/// Arguments de [`start_catalog`] : le nœud catalogue n'a besoin que du
/// secret maître du cluster (chiffrement des modèles en transit, voir
/// [`crate::model::server::ModelServer`]) et d'une poignée Postgres — les
/// migrations (voir [`run_migrations`]) sont appliquées par [`start_catalog`]
/// lui-même, idempotentes, l'appelant n'a qu'à ouvrir le pool (voir
/// [`PgStore::connect`]).
#[derive(TypedBuilder)]
pub struct CatalogArgs {
    secret: SecretManager,
    store: PgStore,
}

/// Démarre un nœud *catalogue* : l'ensemble des catalogues du cluster —
/// modèles, tools, experts, sessions et workspaces — servis par un même
/// nœud, adossés au même stockage Postgres. Regroupement volontaire pour
/// l'instant : chaque domaine reste un serveur indépendant, enregistré sur
/// son propre namespace (la sélection décentralisée des clients — voir
/// `BootstrapClient::select_peer` — ne voit que des namespaces, pas des
/// nœuds), donc les éclater plus tard en nœuds dédiés ne demandera que des
/// fonctions `start_*` plus fines, sans toucher aux clients.
///
/// Un seul [`rpc::RpcServer`] est construit puis cloné entre les cinq
/// serveurs (le clone partage le même acteur/registre, voir
/// `RpcServer::register`) : deux acteurs RPC distincts sur le même transport
/// répondraient chacun `NoExecutorFound` aux appels enregistrés chez
/// l'autre, en course avec la vraie réponse.
///
/// Bloque jusqu'à l'arrêt du réseau — même modèle que
/// [`crate::network::worker::start_worker`].
pub async fn start_catalog(args: CatalogArgs) -> Result<(), anyhow::Error> {
    use super::peer::NodeKind::Catalog;

    let swarm = create_swarm(Catalog)?;
    let local_peer_id = *swarm.local_peer_id();

    let net = NetworkActor::new(swarm, Catalog);

    run_migrations(args.store.pool()).await?;

    // on démarre un client bootstrap qui va enregistrer ce nœud sur les
    // namespaces de chaque catalogue qu'il sert
    let bootstrap = bootstrap::build_client(&net, BootstrapArgs::builder().local_peer_id(local_peer_id).build());

    let rpc_server = rpc::build_server(&net);
    let rpc_client = rpc::build_client(&net);

    // Modèles / tools / experts : catalogues en mémoire (leurs stores
    // durables existent — voir `model::catalog::store` — mais ne sont pas
    // encore branchés sur leurs serveurs). Leurs `::new` ne prennent pas de
    // `BootstrapClient`, contrairement à session/workspace qui s'enregistrent
    // eux-mêmes : on publie leurs namespaces ici, sans quoi
    // `ModelClient::select_catalog` (et homologues) ne désignerait jamais ce
    // nœud.
    ModelServer::new(local_peer_id, rpc_server.clone(), args.secret.clone());
    let _tools = ToolServer::new(rpc_server.clone());
    {
        let mut rpc_server = rpc_server.clone();
        ExpertServer::new(&mut rpc_server);
    }
    bootstrap.register_to_namespaces([
        Namespace::from_static(NS_MODEL),
        Namespace::from_static(NS_TOOL),
        Namespace::from_static(NS_EXPERT),
    ]);

    // Client worker partagé : resoumission des jobs `RunAgent`/`RunGraphStep`
    // débloqués par le serveur de sessions, et dispatch des `ExecuteTool`
    // vers les workers du cluster.
    let worker_client = worker::build_client(
        &net,
        WorkerClientArgs::builder().rpc(rpc_client.clone()).bootstrap(bootstrap.clone()).build(),
    );

    // Dispatch des appels de tools (RPC `ExecuteTool` -> job `ToolExecution`
    // spawné sur un worker, voir `tools::rpc::ExecuteTool`).
    ToolServerActor::new(
        net.transport().chain::<PubSubLayer, _>(()).chain::<ToolEventLayer, _>(()),
        net.transport().chain::<PubSubLayer, _>(()).chain::<WorkerEventLayer, _>(()),
        rpc_server.clone(),
        worker_client.clone(),
    );

    // Sessions : catalogue adossé au store Postgres partagé.
    let _sessions = session::build_server(
        &net,
        SessionServerArgs::builder()
            .rpc_server(rpc_server.clone())
            .bootstrap(bootstrap.clone())
            .worker(worker_client)
            .store(SessionStoreActor::create(args.store.clone()))
            .build(),
    );

    // Workspaces : même modèle que les sessions, même store partagé.
    let _workspaces = workspace::build_server(
        &net,
        WorkspaceServerArgs::builder()
            .rpc_server(rpc_server)
            .bootstrap(bootstrap)
            .store(WorkspaceStoreActor::create(args.store))
            .build(),
    );

    net.clone().listen(true).await;

    Ok(())
}
