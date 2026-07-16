use std::{collections::HashMap, sync::Arc};

use futures::{SinkExt as _, StreamExt as _};
use libp2p::PeerId;
use parking_lot::Mutex;
use tokio::{select, sync::mpsc};

use crate::{id, layer::Layer, rpc::{RpcClient, RpcEvent, RpcEventKind, RpcServer}, sink::SinkBoxExt};

pub struct RpcRegisterActor;

#[derive(Default, Clone)]
pub struct RpcRegistry{
    infos: Arc<Mutex<HashMap<PeerId, RpcServersInfo>>>,
}

impl RpcRegistry {
    /// Trouve le meilleur candidat pour exécuter la RPC
    pub fn find_candidates(&self, name: impl ToString) -> Vec<PeerId> {
        let name = name.to_string();

        self.infos.lock()
            .iter()
            .filter(move |(_, infos)| {
                infos.can_execute.contains(&name)
            })
            .filter(|(_, infos)| infos.status == RpcServerStatus::Alive)
            .map(|(peer_id, _)| *peer_id)
            .collect()
    }

    pub fn can_execute(&self, peer_id: PeerId, names: impl IntoIterator<Item=String>) {
        let mut guard = self.infos.lock();
        let info = guard.entry(peer_id).or_default();
        info.can_execute.extend(names.into_iter());
        info.can_execute.dedup();
    }

    pub fn remove(&self, peer_id: PeerId) {
        self.infos.lock().remove(&peer_id);
    }

    pub fn insert(&self, peer_id: PeerId, info: RpcServersInfo) {
        self.infos.lock().insert(peer_id, info);
    }
}


pub struct RpcActor;

impl RpcActor {
    pub fn new(
        layer: impl Layer<Send=super::RpcEvent, Received = RpcEvent>, 
        local_peer_id: PeerId, 
        registry: RpcRegistry,
        client: RpcClient,
        server: RpcServer
    ) -> RpcService {
        let (tx, rx) = layer.split();
        let mut tx = tx.boxed_sink();
        let mut rx = rx.boxed();

        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<RpcEvent>();

        let _ = event_tx.send(RpcEvent { 
            id: String::default(),
            source: local_peer_id, 
            kind: super::RpcEventKind::Joined { can_execute: server.can_execute() } 
        });
        

        let reg0 = registry.clone();
        
        tokio::spawn(async move {
            loop {
                select! {
                    Some(event) = event_rx.recv() => {
                        let _ = tx.send(event);
                    },
                    Some(event) = rx.next() => {
                        match event.kind {
                            RpcEventKind::Metrics() => todo!(),
                            RpcEventKind::CanExecute(names) => {
                                reg0.can_execute(event.source, names);
                            },
                            RpcEventKind::Joined { can_execute } => {
                                reg0.can_execute(event.source, can_execute);
                            },
                        }
                    }
                }
            }
        });

        RpcService { event_tx, local_peer_id, registry, client, server }

    }
}

#[derive(Default)]
pub struct RpcServersInfo {
    status: RpcServerStatus,
    can_execute: Vec<String>
}

#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub enum RpcServerStatus {
    #[default]
    Alive,
    Dead
}

pub struct RpcService {
    /// Canal to send events to the RPC sub-cluster
    event_tx: mpsc::UnboundedSender<RpcEvent>,
    /// The local peer id
    local_peer_id: PeerId,
    /// Shared RPC servers info
    registry: RpcRegistry,
    /// The client service
    client: RpcClient,
    /// The server service
    server: RpcServer
}