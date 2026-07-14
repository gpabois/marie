# Marie

**Marie** est un runtime de cluster pair-à-pair (Rust, [libp2p](https://libp2p.io)) pour exécuter des agents LLM répartis sur plusieurs nœuds — sans serveur central, sans base de données externe obligatoire, sans configuration réseau lourde. Les nœuds se découvrent automatiquement sur le réseau local (mDNS) ou via `gossipsub`, s'authentifient mutuellement via un secret de cluster partagé, et se répartissent le travail.

`marie-core` est une **bibliothèque**, pas un binaire prêt à l'emploi : elle fournit les briques (rôles de nœud, catalogues, sessions, orchestration) que votre application assemble pour construire son propre cluster ou sa propre passerelle (HTTP, WebSocket, CLI...).

## État du projet

Ce dépôt est un travail en cours. La couche réseau, la réplication d'état (Raft), la synchronisation de session (CRDT), les catalogues, le human-in-the-loop et l'arrêt propre sont fonctionnels et testés. La boucle d'exécution d'un agent (appel modèle → dispatch des tool calls → réponse, ou yield sur une question posée à un humain) est câblée et branchée pour un agent en mode `Simple` (voir `agent::run` et `network::worker::mod::run_simple`), de même que le pilotage d'un `StateGraph` (voir plus bas), y compris un nœud qui délègue à un agent du catalogue d'experts (`Executable::Agent`). **`SessionMode::Orchestration` (un agent qui délègue à des agents enfants) n'est en revanche pas encore branché côté worker** — la coordination des enfants (attente, agrégation des résultats) reste un `todo!()`. Autre limite connue : rien ne persiste encore le contenu d'une réponse humaine (`HumanInputAnswer`) le temps qu'un agent yieldé la retrouve à sa reprise — seule la corrélation (quel agent reprendre) est câblée. Ne partez pas de ce projet en production sans avoir vérifié l'état exact du code sur les parties qui vous intéressent.

Un système de fichiers virtuel (`/var`, `/files`, `/session`, voir plus bas) est disponible depuis peu : un `NodeRole::Worker`/`NodeRole::Persistency` a donc désormais aussi besoin d'un pool PostgreSQL (arborescence `/files` et alias), en plus du backend `ObjectStore` déjà requis pour son contenu.

## Concepts clés

| Rôle | Rôle joué dans le cluster |
|---|---|
| `ControlPlane` | Cluster Raft répliquant l'ordonnancement des jobs et les catalogues (modèles, tools, experts). Élit un leader automatiquement, sans configuration manuelle. |
| `Worker` | Exécute les jobs (`RunAgent`) que lui assigne le control plane. |
| `Persistency` | Détenteur durable de secours pour le contenu des sessions (CRDT), interrogé par un worker qui reprend une session à froid. |
| `Client` | Nœud tiers qui se contente de rejoindre le réseau (`Marie::join`) — typiquement une passerelle HTTP/WebSocket, un tableau de bord, ou une passerelle human-in-the-loop. |

Trois notions à ne pas confondre :
- Un **job** est un run *borné* : une seule tentative d'exécution, qui se termine toujours (`Completed`, `Failed`, ou `Yielded` si l'agent doit attendre quelque chose d'externe). Un job ne redevient jamais `Pending` — reprendre un agent après un yield ou un échec revient à soumettre un *nouveau* job.
- Un **agent** (`GlobalAgentId`) est ce qui vit dans la durée : son état (contexte, statut, mode courant) est persisté dans le CRDT de sa session, et peut être piloté par plusieurs jobs successifs au fil du temps.
- Un **workspace** (`WorkspaceId`) regroupe plusieurs sessions et porte un état partagé entre elles (contexte commun, store clé-valeur) — c'est aussi la *seule* manière de faire naître une session (`WorkspaceClient::create_session`, voir plus bas) : une session n'existe jamais sans workspace.

## Installation

```toml
[dependencies]
marie-core = { path = "../marie-core" }  # ou une référence git/crates.io selon votre projet
sqlx = { version = "0.9", default-features = false, features = ["runtime-tokio", "tls-rustls-ring", "postgres"] }  # requis pour NodeRole::Worker/Persistency (voir le VFS, plus bas)
```

## Démarrage rapide

L'exemple ci-dessous démarre, dans un seul processus (à des fins de démonstration — en pratique chaque rôle tournerait sur sa propre machine), un cluster minimal : un control plane, un worker, et un nœud de persistance, tous connectés via le même secret de cluster.

```rust
use std::sync::Arc;

use marie_core::{
    Marie, MarieConfig, NodeRole,
    mode::executable::RustRegistry,
    network::cp::log_store::redb_backend::RedbLogBackend,
    persistency::{FilesystemConfig, RedbStore},
};
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Le même secret doit être partagé par tous les nœuds d'un même cluster —
    // c'est lui, pas l'identité libp2p (régénérée à chaque démarrage), qui
    // les authentifie mutuellement. À remplacer par 32 octets réellement
    // aléatoires en pratique (ex: générés une fois puis chargés depuis un
    // secret manager) — valeur fixe ici pour que l'exemple soit reproductible.
    let master_key: [u8; 32] = [0x42; 32];
    let config = MarieConfig::builder().master_key(master_key).build();

    // Backends du VFS des sessions (voir plus bas), partagés par tous les
    // workers/nœuds de persistance du cluster : un pool PostgreSQL pour
    // l'arborescence de `/files` et les alias, un `ObjectStore` pour le
    // contenu des fichiers (mémoire ici — S3/compatible S3 en pratique via
    // `FilesystemConfig::S3`). À remplacer par votre chaîne de connexion réelle.
    let pool = PgPoolOptions::new().connect("postgres://localhost/marie").await?;
    let object_store = FilesystemConfig::Memory.build()?;

    // --- Control plane ---
    let cp = Marie::new(MarieConfig::builder().master_key(master_key).build());
    let catalogs_store = Arc::new(RedbStore::open("catalogs.redb")?);
    let raft_log = Arc::new(RedbLogBackend::open("raft-log.redb")?);
    let cp_handle = cp.start(NodeRole::ControlPlane {
        raft_log_backend: raft_log,
        model_store: catalogs_store.clone(),
        tool_store: catalogs_store.clone(),
        expert_store: catalogs_store.clone(),
        state_graph_store: catalogs_store,
    });

    // --- Worker ---
    let worker = Marie::new(config);
    let rust_registry = RustRegistry::new(); // fonctions Rust pour vos StateGraph, voir plus bas
    let worker_handle = worker.start(NodeRole::Worker { pool: pool.clone(), store: object_store.clone(), rust_registry });

    // --- Persistency ---
    let persistency = Marie::new(MarieConfig::builder().master_key(master_key).build());
    let session_store = Arc::new(RedbStore::open("sessions.redb")?);
    let persistency_handle =
        persistency.start(NodeRole::Persistency { store: session_store, pool: pool.clone(), object_store: object_store.clone() });

    // Laisse le temps à mDNS de découvrir les pairs et au cluster Raft de s'initialiser.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // ... votre application ...

    // Arrêt propre : draine le travail en vol avant de couper le réseau.
    worker_handle.shutdown().await;
    persistency_handle.shutdown().await;
    cp_handle.shutdown().await;

    Ok(())
}
```

## Catalogues : modèles, tools, experts

Trois catalogues, tous répliqués via Raft et gérés avec le même CRUD (`get`/`list`/`set`/`remove`), accessibles depuis n'importe quel nœud connecté :

```rust
use marie_core::model::declaration::Model;

let models = cp.model_client()?; // ou depuis n'importe quel nœud connecté (Marie::join, un worker, ...)

models.set("gpt-main", Model::OpenAICompatible {
    base_url: "https://api.openai.com/v1".into(),
    client_id: "org-...".into(),
    api_key: "sk-...".into(),      // ne transite jamais en clair sur le réseau
    model: "gpt-4.1".into(),
    system_prompt: Some("Tu es un assistant utile.".into()),
}).await?;

let declaration = models.get("gpt-main").await?;
```

`Model` est un enum plutôt qu'une struct : `OpenAICompatible` est la seule variante aujourd'hui, mais la forme laisse la place à d'autres protocoles d'accès (une authentification différente, par exemple) sans casser les catalogues existants.

Un **expert** combine un prompt, un modèle et une liste de tools autorisés — un préréglage d'agent réutilisable :

```rust
use marie_core::expert::declaration::ExpertDeclaration;

cp.expert_client()?.set("support-client", ExpertDeclaration {
    prompt: "Tu réponds aux questions du support client, poliment et brièvement.".into(),
    model_id: "gpt-main".into(),
    allowed_tools: vec!["search-kb".into()],
}).await?;
```

Les tools suivent le même schéma (`ToolDeclaration { signature, scope }`), avec en plus un exécuteur enregistré dynamiquement par le nœud qui sait effectivement l'exécuter :

```rust
use marie_core::tools::ToolCallResponse;

let tools = worker.tool_client()?; // n'importe quel nœud connecté

tools.register_executor("search-kb", |_request| async move {
    // ... exécute la recherche ...
    Ok(ToolCallResponse::Success { output: None })
}).await?;
```

## Modes de session : simple, orchestration, graphe d'état

Une session peut empiler plusieurs modes de fonctionnement au fil de son exécution — `Simple` (le mode implicite, conversation directe avec le modèle), `Orchestration` (un agent délègue à des agents enfants), ou `StateGraph` (l'exécution suit un graphe d'états explicite). On empile/dépile via `SessionClient` (piloté par un tool dédié, `system/push-mode` / `system/pop-mode`, ou directement par un humain/une passerelle) :

```rust
use marie_core::mode::{SessionMode, state_graph::{StateGraph, Node, Edge}, executable::{Executable, RustRegistry, NodeOutcome}};

// Le graphe et ses fonctions Rust — `registry` est celui passé à
// `NodeRole::Worker` au démarrage (voir le démarrage rapide), à conserver
// pour continuer à y enregistrer des fonctions après coup.
registry.register_node("greet", |_input| async move {
    Ok(NodeOutcome::Value(serde_json::json!("bonjour")))
});

let graph = StateGraph::new(
    vec![Node::new("start", Some(Executable::Rust { id: "greet".into() })), Node::new("end", None)],
    vec![Edge::new("start", "end", None)],
    "start",
)?;

// `Marie::session_client` construit (ou réutilise) le `SessionClient` de ce
// nœud, à partir des mêmes `pool`/`object_store` que `NodeRole::Worker` (voir
// le démarrage rapide) — `session_id` doit déjà exister (voir
// `WorkspaceClient::create_session`, section suivante).
let sessions = worker.session_client(pool.clone(), object_store.clone())?;
sessions.push_mode(session_id, SessionMode::StateGraph(graph)).await?;
```

Un nœud (jamais une arête) peut aussi déléguer à un agent du catalogue d'experts plutôt qu'à du code (`Executable::Agent { expert_id, task }`) — l'expert (son prompt, son modèle, ses tools autorisés) est résolu au moment de l'exécution, `task` est la tâche spécifique confiée pour ce nœud précis :

```rust
use marie_core::mode::executable::Executable;

let node = Node::new("summarize", Some(Executable::Agent {
    expert_id: "support-client".into(),
    task: "Résume la conversation en 3 points.".into(),
}));
```

Les nœuds/arêtes d'un graphe peuvent aussi référencer une fonction Rust déjà enregistrée (`Executable::Rust`, exécutée localement sur le worker), ou porter du code source Python/Rune (`Executable::Python`/`Executable::Rune`) — ces deux dernières variantes ne sont pour l'instant que des données : aucun interpréteur n'est embarqué dans `marie-core`, à vous de le brancher si besoin.

## Human-in-the-loop

Un agent peut soumettre un formulaire à un humain (texte court/long, choix unique ou multiple, upload de fichier) et attendre sa réponse, sans être bloqué par les délais habituels du réseau — le transport est découplé du reste, spécifiquement pensé pour une latence humaine potentiellement longue.

Côté agent :

```rust
use marie_core::hitl::Question;

let hitl = worker.hitl_client()?; // n'importe quel nœud connecté

let answers = hitl.ask(agent_id, vec![
    Question::select("env", "Sur quel environnement déployer ?", vec!["staging".into(), "prod".into()]),
    Question::long_text("notes", "Notes additionnelles (optionnel)"),
]).await?;
```

Côté passerelle humaine (typiquement un nœud `Marie::join()`, ex. une interface web) :

```rust
use futures::StreamExt as _;
use marie_core::hitl::Answer;

let gateway = Marie::new(MarieConfig::builder().master_key(master_key).build());
let (_network_client, _handle) = gateway.join().await?;
let hitl = gateway.hitl_client()?;

let mut requests = hitl.subscribe_requests();
while let Some(request) = requests.next().await {
    // ... présentez `request.questions` à un opérateur ...
    let answers: std::collections::HashMap<String, Answer> = /* réponses recueillies */ Default::default();
    hitl.answer(&request, answers).await?;
}
```

## Système de fichiers virtuel : `/var`, `/files`, `/session`

Chaque workspace expose un petit système de fichiers virtuel (`VFS`), point d'accès unifié à son état et à ses fichiers :

- `/var` — store clé-valeur plat du workspace (`YrsWorkspace::state`, CRDT, gossipé) : `/var/foo/bar` correspond à la clé `foo.bar` ; écrire `1` y stocke le nombre `1`, pas la chaîne `"1"` (le contenu écrit est interprété comme du JSON si possible, sinon comme une chaîne brute).
- `/files` — fichiers du workspace, adossés à un `ObjectStore` pour le contenu et à un catalogue d'inodes PostgreSQL pour l'arborescence (dossiers, listage).
- `/session` — la session courante montée dans le même espace : `/session/var` (store clé-valeur propre à la session) et `/session/files` (fichiers propres à la session — un sous-arbre du catalogue d'inodes du workspace, pas un stockage séparé).

Une session n'existe **que** rattachée à un workspace — c'est ce qui garantit que `/session/files` a toujours une racine où se rattacher :

```rust
let workspaces = worker.workspace_client()?;
workspaces.acquire(workspace_id).await?; // ou création si jamais vu, voir WorkspaceClient::acquire

let session_id = workspaces.create_session(workspace_id).await?; // seul moyen de créer une session

let sessions = worker.session_client(pool.clone(), object_store.clone())?;
sessions.acquire(session_id).await?;
```

Le VFS complet d'une session (`SessionClient::vfs`) s'utilise comme un système de fichiers ordinaire (`mkdir`/`ls`/`open`/`remove`, voir `persistency::filesystem::FileSystem`) :

```rust
use marie_core::persistency::filesystem::{FileSystem as _, OpenOptions};

let vfs = sessions.vfs(session_id).await?;
vfs.mkdir("/session/files/reports").await?;
vfs.ls("/var").await?; // variables du workspace, premier niveau

let mut file = vfs.open("/session/files/reports/q1.md", OpenOptions::builder().create(true).write(true).build()).await?;
```

Pour `/session/files` spécifiquement, `SessionClient` expose aussi des raccourcis (`read_file`/`write_file`/`delete_file`/`list_files`) qui font l'aller-retour `vfs()` + ouverture/fermeture du descripteur pour vous :

```rust
sessions.write_file(session_id, "reports/q1.md", b"...".to_vec()).await?;
let content = sessions.read_file(session_id, "reports/q1.md").await?; // None si absent
```

Un alias fait pointer un chemin vers un autre, comme un lien symbolique sur un dossier — utile pour exposer un raccourci stable sans dupliquer les fichiers :

```rust
vfs.alias("/current", "/session/files").await?; // /current/rapport.md == /session/files/rapport.md
```

## Persistance : redb par défaut, autre backend au choix

Tout ce qui doit survivre à un redémarrage passe par un trait abstrait, pas par un moteur de stockage imposé :

- `persistency::store::Store<T>` pour les catalogues et le contenu de session (implémentation par défaut : `RedbStore`, embarqué, sans serveur à administrer).
- `network::cp::log_store::RaftLogBackend` pour le log Raft du control plane (implémentation par défaut : `RedbLogBackend`).

Les deux sont des traits publics : vous pouvez fournir votre propre implémentation (Postgres, sled, etc.) sans toucher au reste de la bibliothèque.

## Arrêt propre

`MarieHandle::shutdown()` est la manière recommandée d'arrêter un nœud : elle draine le travail en vol (un worker laisse ses jobs en cours se terminer, jusqu'à 30 secondes) avant de couper la connexion réseau. `MarieHandle::abort()` reste disponible pour un arrêt immédiat sans garantie, si le nœud ne répond déjà plus.

## Licence

Non définie pour l'instant.
