# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Marie is a peer-to-peer cluster runtime (Rust, libp2p) for running LLM agents across nodes. Nodes discover each other via mDNS/gossipsub, authenticate mutually via a shared cluster secret (not their libp2p identity, which is regenerated on every restart), and take on one of several roles: `ControlPlane` (Raft-replicated scheduling/catalogs), `Worker` (executes agent jobs), or `Persistency` (durable backup of session state). A `Client` role lets third-party code (e.g. an HTTP/WebSocket gateway) join the network without taking on a cluster role.

The workspace has two crates:
- `marie-core` — the library: everything above.
- `marie-test` — integration tests (`marie-test/tests/*.rs`) exercising `marie-core` as an external consumer.

There is no `main.rs`/binary yet; `marie-core::Marie` (see `node/mod.rs`) is the entry point library consumers build on.

## Commands

```sh
cargo check --workspace          # fastest correctness check
cargo build --workspace
cargo test --workspace           # unit tests (in marie-core) + integration tests (marie-test)
cargo test -p marie-test --test crdt              # a single integration test file
cargo test -p marie-test --test crdt test_sync_via_diff   # a single test
cargo test -p marie-core session::crdt::tests     # unit tests inline in marie-core (e.g. session/crdt.rs)
```

## Architecture

### Node roles and startup (`node/mod.rs`, `network/mod.rs`)

`Marie::start(role)` spawns one of three long-running role loops in the background, each opening its own libp2p swarm via `network::start_swarm`:
- `network::cp::start_control_plane` — Raft cluster (via `openraft`) replicating a `ControlPlaneState` (model catalog, tool catalog, worker registry, job assignments, session holders). Bootstrap leader is elected deterministically (lowest derived node id among peers seen within `BOOTSTRAP_DELAY`), not via a Raft election message.
- `network::worker::start_worker` — executes `RunAgent` jobs pushed by the control plane, synchronizing the target session's CRDT state before running.
- `network::persistency::start_persistency` — passively ingests session CRDT diffs off gossipsub into durable storage and serves `FETCH_SESSION` for cold recovery.

`Marie::join()` is the lighter-weight entry point for a third-party node that just wants a `NetworkClient` (RPCs, event subscription) without running a role loop.

All roles authenticate each other using `SecretManager` (HKDF/HMAC over a shared cluster master key, see `secret.rs`) — `agent_version` announced via libp2p `identify` is self-declared and untrusted; peers claiming `ControlPlane` must additionally answer an HMAC challenge (`network::actor::NetworkActor::challenge_control_plane`) before being trusted.

### Networking core (`network/actor.rs`)

`NetworkActor` owns the single-threaded libp2p `Swarm` and is driven by `NetworkActor::run`. `NetworkClient` (cheap to clone) is the handle other code uses to talk to it: request/response RPCs (`rpc`/`rpc_to`), gossipsub pub/sub, dynamically-registered RPC handlers (`register_rpc`), and a broadcast `NetworkEvent` stream (`subscribe_events`) that multiple independent consumers can each subscribe to (e.g. a role's main loop and `SessionClient` both listen independently).

Two gossipsub topics run through the same `node_gossip`/`worker_gossip` behaviours but are logically distinct: `RPC_REGISTRY_TOPIC` (control-plane-to-control-plane, propagates dynamically registered RPC names) and session-specific topics (see below).

RPCs not natively known by a control plane (`execute_rpc`'s catch-all arm) are looked up in a `DynamicRpcRegistry` and raced across all registered executors (`forward_race`) — first responder wins, others are dropped.

### Sessions and CRDT sync (`session/`, `network/worker/session_client.rs`)

An agent's live state (frames, streamed stdio/stderr, context, logs) lives in a `YrsSession` — a Yjs/`yrs` CRDT document, not a Raft-replicated struct — because it's large and written continuously, and a job may be reassigned to a different worker mid-flight. Only job *assignment* goes through Raft (`ControlPlaneState`); session *content* is exchanged directly between peers as incremental diffs over gossipsub (`session::sync::SESSION_SYNC_TOPIC`).

Key invariant: a worker that has never seen a session must not call `YrsSession::new` and then apply a diff onto it — that creates a concurrent, conflicting CRDT root. It must instead start from an empty `Doc` and reconstruct via `YrsSession::from_diff`/`open` (see the doc comments on `YrsSession::from_diff`). `SessionClient::acquire` and `network::persistency::ingest_session_diff` both follow this rule.

`SessionClient` (in `network/worker/session_client.rs`) is the bridge workers use: `acquire` syncs from `known_holders` (tried in order) or creates a fresh session if none exist, then keeps it live via `SESSION_SYNC_TOPIC` for the rest of its life — no need to re-fetch from every holder, just one to bootstrap. Session *lifecycle* events (`Created`/`FrameStatusChanged`/`LogAppended`/`Removed`) are a separate, much lower-volume gossip topic (`SESSION_EVENTS_TOPIC`) from the CRDT content diffs.

`ControlPlaneState::session_holders`/`network::cp::session_holders_for` compute who currently holds a session's CRDT state (for handing to a newly (re)assigned worker) by combining Raft-replicated `Job::Running` state with the current reconcile pass's not-yet-committed assignments, plus known `Persistency` nodes as a last-resort fallback.

### Control plane scheduling (`network/cp/mod.rs`)

`reconcile` runs on a fixed interval (`RECONCILE_INTERVAL`) on every control plane node, but only the current Raft leader's writes actually stick (`propose_best_effort` fails silently if not leader — cheap, since every node runs the same reconcile logic and the real leader will succeed). It: healthchecks known workers via raw libp2p connectivity (no application-level ping), reassigns jobs whose worker just went dark, and assigns `Pending` jobs to healthy idle workers.

Two write paths into `ControlPlaneState` exist: `propose_best_effort` (internal triggers — peer discovery, scheduling — no caller waiting, silently no-ops off-leader) and `propose_or_forward` (RPC-driven — a caller wants a definitive answer, so a non-leader node forwards the original RPC to the current leader with retries).

### Secrets (`secret.rs`)

`SecretManager` holds the cluster master key and derives everything else via HKDF: per-node keys (`derive_node_key`, keyed by libp2p `PeerId` — used for encrypting a payload so only a specific peer can decrypt it, e.g. model API keys in transit) and a fixed at-rest storage key (`derive_storage_key`, deliberately peer-independent and stable across restarts, so a node can decrypt what it persisted before its `PeerId` changed). The master key never crosses the network — membership is proven via HMAC challenge/response (`prove_membership`/`verify_membership`), not by exchanging the key.

### Persistence (`persistency/`)

`Store<T>` (see `persistency/store.rs`, backed by `RedbStore`/`redb`) is the generic local KV abstraction used for cold-start recovery of the model/tool catalogs (`network::cp::load_catalog_from_store`) and for `SessionStore`. `SessionFilesystem` (`persistency/filesystem.rs`) is separate and already cluster-shared (memory or S3-compatible via `object_store`) — session *files* don't need CRDT sync or `acquire`, only session *state* does.

### IDs (`id.rs`)

`ID` is a 128-bit value (two `u64`s: a per-`IdGenerator`-instance random session prefix + a sequential/random local part), not a UUID crate type. `IdGenerator` is seeded from OS randomness by default; tests construct it directly for deterministic-enough (session-scoped) ids. `ID::to_string`/`FromStr` use a fixed 32-hex-char representation — don't assume UUID formatting elsewhere in the codebase.

## Code comments

Existing doc comments (`///`) throughout the codebase are written in French and are unusually substantive — they frequently record *why* a design was chosen over an alternative (e.g. CRDT vs Raft for sessions, gossip vs Raft for the RPC registry, best-effort vs forwarded writes). Read them before changing the surrounding code, and match the existing language/depth when extending a module that already has them rather than switching to terse English comments.
