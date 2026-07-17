use std::{sync::Arc, collections::HashMap};
use futures::{StreamExt, sink::SinkExt, stream::{BoxStream, SelectAll}};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use libp2p::{PeerId, Swarm, rendezvous::Namespace};
use tokio::{select, sync::mpsc, time::interval};
use typed_builder::TypedBuilder;

use crate::{layer::Layer, network::{MarieBehaviour, actor::{NetworkCommand, NetworkEvent}}, sink::SinkBoxExt};

#[derive(TypedBuilder)]
pub struct BootstrapArgs {
    local_peer_id: PeerId,
    #[builder(default)]
    namespaces: Vec<Namespace>
}

pub struct BootstrapClientActor;

pub enum PeerSelection {
    Local,
    Peer(PeerId)
}

#[derive(Clone)]
pub struct BootstrapClient {
    local_peer_id: PeerId, 
    tracked: Arc<Mutex<HashMap<String, Vec<PeerId>>>>,
    cmd_tx: mpsc::UnboundedSender<Command>
}

impl BootstrapClient {
    pub fn register_to_namespaces(&self,  namespaces: impl IntoIterator<Item=Namespace>) {
        let _ = self.cmd_tx.send(Command::RegisterToNamespaces(namespaces.into_iter().collect()));
    }

    pub fn peers(&self, namespace: impl ToString) -> Vec<PeerId> {
        let guard = self.tracked.lock();
        guard.get(&namespace.to_string()).cloned().unwrap_or_default()
    }

    /// Selectionne une paire parmis le sous-cluster de manière déterministe 
    /// et décentralisée par la méthode du `Hachage cohérent`.
    pub fn select_peer(&self, namespace: impl ToString, id: impl AsRef<[u8]>) -> Option<PeerId> {
        let peers = self.peers(namespace);
        
        peers.iter()
            .map(|peer| {
                let mut hasher = Sha256::default();
                hasher.update(id.as_ref());
                hasher.update(peer.to_bytes());
                let score = hasher.finalize();

                (*peer, score)
            })
            .max_by(|(_, score_a), (_, score_b)| score_a.cmp(score_b))
            .map(|(peer, _)| peer)
    }

    pub fn select_peer_with_local(&self, namespace: impl ToString, id: impl AsRef<[u8]>) -> PeerSelection {
        let mut peers = self.peers(namespace);
        peers.push(self.local_peer_id);

        peers.iter()
            .map(|peer| {
                let mut hasher = Sha256::default();
                hasher.update(id.as_ref());
                hasher.update(peer.to_bytes());
                let score = hasher.finalize();

                (*peer, score)
            })
            .max_by(|(_, score_a), (_, score_b)| score_a.cmp(score_b))
            .map(|(peer, _)| {
                if peer == self.local_peer_id {
                    PeerSelection::Local
                } else {
                    PeerSelection::Peer(peer)
                }
            })
            .unwrap_or(PeerSelection::Local)
    }
}

fn create_timer_stream(duration: std::time::Duration, args: (String, PeerId)) -> BoxStream<'static, (String, PeerId)> {
    let mut timer = interval(duration);
    // Optionnel : évite l'accumulation de ticks si le CPU est surchargé
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    Box::pin(futures::stream::unfold((timer, args), |(mut t, args)| async move {
        t.tick().await; // Attend le prochain tick
        let item = args.clone();
        Some((item, (t, args))) // Retourne l'action et l'état (timer + args) pour le prochain tour
    }))
}

enum Command {
    RegisterToNamespaces(Vec<Namespace>)
}

struct NsPeerInfo {
    peer_id: PeerId,
    expires_at: std::time::Instant,
}

impl BootstrapClientActor {
    pub fn new(layer: impl Layer<Send = NetworkCommand, Received = NetworkEvent>, args: BootstrapArgs) 
        -> BootstrapClient
    {
        let (tx, rx) = layer.split();

        let mut tx = tx.boxed_sink();
        let mut rx = rx.boxed();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<Command>();

        let mut namespaces: Vec<Namespace> = args.namespaces;

        let state: Arc<Mutex<HashMap<String, Vec<PeerId>>>> = Default::default();

        let stat0 = state.clone();
        tokio::spawn(async move {
            let mut ttl_ticks: SelectAll<BoxStream<'static, (String, PeerId)>> = SelectAll::new();
            let mut checks: HashMap<String, Vec<NsPeerInfo>> = Default::default();

            let mut bootstrap_peer_ids = Vec::<PeerId>::default();
            loop {
                select! {
                    Some((ns, peer_id)) = ttl_ticks.next() => {
                        let peers = checks.entry(ns.clone()).or_default();
                        let Some(index) = peers.iter().position(|info| info.peer_id == peer_id) else { continue };
                        let now = std::time::Instant::now();

                        // la pair a expirée, on la jette
                        if peers[index].expires_at > now {
                            // retire le pair de la liste des pairs dans chaque espace de nom.
                            let mut guard = stat0.lock();
                            let entries = guard.entry(ns.clone()).or_default();
                            entries.retain(|p| peer_id != *p);
                            peers.remove(index);
                        }
                    },
                    Some(cmd) = cmd_rx.recv() => {
                        match cmd {
                            Command::RegisterToNamespaces(ns) => {
                                namespaces.extend(ns.into_iter());
                                bootstrap_peer_ids
                                .iter()
                                .for_each(|&bootstrap_peer_id| {
                                    let _ = tx.send(NetworkCommand::RegisterPeer { 
                                        namespaces: namespaces.clone(), 
                                        bootstrap_peer_id, 
                                        ttl: None 
                                    });
                                });
                                
                            },
                        }
                    },
                    Some(event) = rx.next() => {
                        match event {
                            NetworkEvent::PeerDisconnected {peer_id} => {
                                // retire le pair de la liste des serveurs bootstrap.
                                bootstrap_peer_ids.retain(|p| peer_id != *p);

                                // retire le pair de la liste des pairs dans chaque espace de nom.
                                let mut guard = stat0.lock();
                                guard.iter_mut().for_each(|(_, entries)| {
                                    entries.retain(|p| peer_id != *p);
                                });
                            },
                            NetworkEvent::NamespacePeerRegistred {peer_id, namespace, ttl} => {
                                let namespace = namespace.to_string();

                                let peers = checks.entry(namespace.clone()).or_default();
                                let ttl = std::time::Duration::from_secs(ttl);
                                if let Some(info) = peers.iter_mut().find(|info| info.peer_id == peer_id) {
                                    info.expires_at = std::time::Instant::now() + ttl;
                                } else {
                                    peers.push(NsPeerInfo {
                                        peer_id,
                                        expires_at: std::time::Instant::now() + ttl,
                                    });
                                }
                                
                                // on va vérifier si la pair n'a pas expirée après ttl secondes.
                                ttl_ticks.push(create_timer_stream(ttl, (namespace, peer_id)));
                            },
                            NetworkEvent::BootstrapDiscovered { peer_id } => {
                                bootstrap_peer_ids.push(peer_id);
                                let _ = tx.send(NetworkCommand::RegisterPeer{
                                    namespaces: namespaces.clone(), 
                                    bootstrap_peer_id: peer_id, 
                                    ttl: None
                                });
                            },
                            _ => {}
                        }
                    }
                }
            }
        });

        BootstrapClient{
            cmd_tx,
            local_peer_id: args.local_peer_id, 
            tracked: state
        }
    }
}

pub async fn start_bootstrap() -> Result<Swarm<MarieBehaviour>, anyhow::Error> {
    todo!()
}