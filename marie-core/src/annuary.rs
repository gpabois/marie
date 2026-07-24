use std::{collections::HashMap, ops::Deref, sync::Arc};

use futures::{SinkExt, StreamExt, stream::{BoxStream, SelectAll}};
use libp2p::{PeerId, rendezvous::Namespace};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use tokio::{select, sync::mpsc, time::interval};

use crate::{di::{Factory, Get}, layer::Layer, network::{LocalPeerId, Network, protocol::{NetworkCommand, NetworkEvent}}};

pub trait AnnuaryStrategy: Send + Sync + 'static {
    fn register_to_namespaces(&self, namespaces: Vec<String>);
    fn ns_peers(&self, namespace: &str) -> Vec<PeerId>;

    fn select(&self, namespace: &str, key: &[u8]) -> Vec<PeerId> {
        let peers = self.ns_peers(namespace);

        let mut peers: Vec<(PeerId, Vec<u8>)> = peers.iter()
            .map(|peer| {
                let mut hasher = Sha256::default();
                hasher.update(key);
                hasher.update(peer.to_bytes());
                let score = hasher.finalize().to_vec();
                
                (*peer, score)
            })
            .collect();

        peers.sort_by_key(|(_, score)| score.clone());
        peers.into_iter().map(|(id, _)| id).collect()
    }

}

#[derive(Clone)]
pub struct Annuary(Arc<dyn AnnuaryStrategy>);

impl Deref for Annuary {
    type Target = dyn AnnuaryStrategy;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

pub struct LoopbackAnnuary(LocalPeerId);

impl AnnuaryStrategy for LoopbackAnnuary {
    fn ns_peers(&self, namespace: &str) -> Vec<PeerId> {
        vec![*self.0]
    }

    fn select(&self, namespace: &str, key: &[u8]) -> Vec<PeerId> {
        vec![*self.0]
    }
    
    fn register_to_namespaces(&self, namespaces: Vec<String>) {}
}

#[derive(Clone)]
pub struct SwarmAnnuary {
    tracked: Arc<Mutex<HashMap<String, Vec<PeerId>>>>,
    cmd_tx: mpsc::UnboundedSender<Command>
}

struct NsPeerInfo {
    peer_id: PeerId,
    expires_at: std::time::Instant,
}

impl AnnuaryStrategy for SwarmAnnuary {
    fn ns_peers(&self, namespace: &str) -> Vec<PeerId> {
        let guard = self.tracked.lock();
        guard.get(&namespace.to_string()).cloned().unwrap_or_default()
    }

    fn register_to_namespaces(&self, namespaces: Vec<String>) {
        let _ = self.cmd_tx.send(Command::RegisterToNamespaces(
            namespaces
            .into_iter()
            .map(Namespace::new)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
        ));
    }
}

impl<C> Factory<C> for SwarmAnnuary 
    where C: Get<Network>
{
    fn create(container: &C) -> Self {
        Self::new(container)
    }
}

impl SwarmAnnuary {
    pub fn new<C>(container: &C) -> SwarmAnnuary
        where C: Get<Network>
    {   
        let net: Network = container.get();
        let (mut tx, mut rx) = net.layer().boxed_split();
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<Command>();

        let mut namespaces: Vec<Namespace> = Default::default();

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

        Self {
            cmd_tx,
            tracked: state
        }
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