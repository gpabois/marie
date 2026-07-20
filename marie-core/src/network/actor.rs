use futures::sink::Sink;
use futures::{Stream, StreamExt as _};
use libp2p::rendezvous::{self, Namespace, Ttl};
use libp2p::{gossipsub, identify, mdns, request_response};
use libp2p::{PeerId, swarm::SwarmEvent};
use tokio::sync::watch;
use tokio::{select, sync::{broadcast, mpsc}};
use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};
use tracing::{warn, info};

use crate::layer::Layer;
use crate::network::mux::Frame;
use crate::network::peer::NodeKind;
use crate::{
    network::MarieSwarm,
};

pub enum NetworkCommand {
    Listen,
    SendFrame(Frame),
    Subscribe(gossipsub::IdentTopic),
    Publish {
        topic: gossipsub::IdentTopic,
        payload: Vec<u8>
    },
    /// Enregistre le pair dans un espace de nom
    /// auprès du serveur bootstrap
    RegisterPeer {
        namespaces: Vec<Namespace>,
        bootstrap_peer_id: PeerId,
        ttl: Option<Ttl>
    },
    Shutdown,
}


#[derive(Clone)]
pub enum NetworkEvent {
    ReceivedFrame(Frame),
    BootstrapDiscovered {
        peer_id: PeerId
    },
    NamespacePeerRegistred {
        namespace: Namespace,
        peer_id: PeerId,
        ttl: Ttl
    },
    PeerDisconnected {
        peer_id: PeerId,
    },
    PubSubReceived {
        id: String,
        topic: String,
        data: Vec<u8>,
        source: PeerId,
    }
}

/// Capacité du canal de diffusion des [`NetworkEvent`] (voir
/// [`NetworkClient::subscribe_events`]). Un abonné qui prend trop de retard
/// perd les événements les plus anciens (voir [`NetworkEventHandler`]) —
/// notamment un `RequestRemoteProcedureExecution`, qui ne sera alors jamais
/// répondu (voir le traitement de `rx.await` dans `NetworkActor::run`).
/// Généreuse pour limiter ce risque en pratique, sans prétendre l'éliminer :
/// un appelant dont la requête est ainsi perdue la retentera de toute façon
/// (voir `FORWARD_RETRY_ATTEMPTS`).
const NETWORK_EVENTS_CAPACITY: usize = 1024;

/// Flux de [`NetworkEvent`] : multi-abonnés (voir
/// [`NetworkClient::subscribe_events`]), contrairement à l'ancien canal
/// mono-consommateur — plusieurs composants indépendants (la boucle
/// applicative du rôle courant, `session::client::SessionClient`,
/// ...) peuvent donc chacun observer le flux complet sans se le disputer.
/// `Lagged` (abonné trop en retard) est absorbé silencieusement : voir
/// [`NETWORK_EVENTS_CAPACITY`] pour les conséquences.
pub struct NetworkReceiver(BroadcastStream<NetworkEvent>);

impl Stream for NetworkReceiver {
    type Item = NetworkEvent;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        loop {
            return match std::pin::Pin::new(&mut self.0).poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(event))) => std::task::Poll::Ready(Some(event)),
                std::task::Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(skipped)))) => {
                    warn!(skipped, "abonné réseau en retard, événements perdus");
                    continue;
                }
                std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
                std::task::Poll::Pending => std::task::Poll::Pending,
            };
        }
    }
}

#[derive(Clone)]
pub struct NetworkSender(mpsc::UnboundedSender<NetworkCommand>);

impl Sink<NetworkCommand> for NetworkSender {
    type Error = anyhow::Error;

    fn poll_ready(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: NetworkCommand) -> Result<(), Self::Error> {
        self.0.send(item)?;
        Ok(())
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_close(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
       std::task::Poll::Ready(Ok(()))
    }
}

pub struct NetworkLayer(NetworkSender, NetworkReceiver);

impl Layer for NetworkLayer {
    type Send = NetworkCommand;
    type Received = NetworkEvent;
    type Sender = NetworkSender;
    type Receiver = NetworkReceiver;

    fn split(self) -> (Self::Sender, Self::Receiver) {
        (self.0, self.1)
    }
}

impl NetworkLayer {
    pub fn split(self) -> (NetworkSender, NetworkReceiver) {
        (self.0, self.1)
    }
}

#[derive(Clone)]
pub struct Network {
    shutdown_signal: watch::Receiver<bool>,
    commands: mpsc::UnboundedSender<NetworkCommand>,
    /// Diffusion des [`NetworkEvent`] de ce nœud — voir [`Self::subscribe_events`].
    events: broadcast::Sender<NetworkEvent>,
    /// Identité libp2p de ce nœud — voir [`Self::decrypt_secret`].
    local_peer_id: PeerId,
}

impl Network {
    /// Récupère la couche de transport du réseau
    pub fn transport(&self) -> NetworkLayer {
        let sender = NetworkSender(self.commands.clone());
        let receiver = NetworkReceiver(BroadcastStream::new(self.events.subscribe()));
        NetworkLayer(sender, receiver)
    }

    /// Connecte le noeud au réseau
    pub async fn listen(mut self) {
        self.commands.send(NetworkCommand::Listen);
        loop {
            select! {
                _ = self.shutdown_signal.changed() => break
            }
        }
    }

    /// S'abonne à un topic gossipsub (`node_gossip`) : les messages publiés
    /// dessus par d'autres nœuds abonnés remonteront via
    /// `NetworkEvent::GossipMessageReceived`.
    pub fn subscribe(&self, topic: impl Into<String>) {
        use NetworkCommand::Subscribe;
        let _ = self.commands.send(Subscribe(gossipsub::IdentTopic::new(topic)));
    }

    /// Arrête [`NetworkActor::run`] de ce nœud — voir
    /// `NetworkCommand::Shutdown`. À appeler une fois qu'il n'y a plus rien
    /// à envoyer/recevoir sur ce `NetworkClient` (voir
    /// `network::cp::start_control_plane`/`network::worker::start_worker`/
    /// `network::persistency::start_persistency`, qui l'appellent en tout
    /// dernier, après avoir drainé leur propre travail en vol) : les
    /// `NetworkCommand` envoyés après coup sont silencieusement perdus,
    /// l'actor n'étant plus là pour les traiter.
    pub fn shutdown(&self) {
        let _ = self.commands.send(NetworkCommand::Shutdown);
    }
}
pub struct NetworkActor {
    shutdown_signal: watch::Sender<bool>,
    kind: NodeKind,
    swarm: MarieSwarm,
    // Diffusion des `NetworkEvent` (voir `NetworkClient::subscribe_events`)
    events_tx: broadcast::Sender<NetworkEvent>,
    // Network command to execute
    commands_rx: mpsc::UnboundedReceiver<NetworkCommand>,
    commands_tx: mpsc::UnboundedSender<NetworkCommand>,
}

impl NetworkActor {
    #[must_use]
    pub fn new(swarm: MarieSwarm, kind: NodeKind) -> Network {
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let (events_tx, _) = broadcast::channel(NETWORK_EVENTS_CAPACITY);
        let (shutdown_subscribers, shutdown_signal) = watch::channel(false);

        let local_peer_id = *swarm.local_peer_id();
        

        let client = Network {
            shutdown_signal,
            commands: commands_tx.clone(),
            events: events_tx.clone(),
            local_peer_id,
        };



        let actor = NetworkActor {
            shutdown_signal: shutdown_subscribers,
            kind,
            swarm,
            events_tx,
            commands_rx,
            commands_tx,
        };

        tokio::spawn(actor.run());

        client
    }

    async fn run(mut self) -> Result<(), anyhow::Error> {

        use NetworkCommand::*;
        use SwarmEvent::Behaviour;
        use request_response::Event as ReqResEvent;
        use identify::Event as IdEvent;
        use super::MarieBehaviourEvent::{PubSub, Identify, Mdns, Oneway, Rendezvous};


        loop {
            select! {
                Some(cmd) = self.commands_rx.recv() => {
                    match cmd {
                        Listen => {
                            self.swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
                            info!("📡 Swarm [{}] initialisé. PeerID: {}", self.kind, self.swarm.local_peer_id());
                        }
                        SendFrame(mut frame) => {
                            // Le frame n'a pas de source, on va l'ajouter.
                            // Le frame peut comporter une source notamment dans un cas de forward.
                            if frame.source.is_none() {
                                frame.source = Some(*self.swarm.local_peer_id());
                            }
                            
                            // on a pas de destinataire, c'est plus compliqué.
                            let Some(dest) = frame.destination.clone() else { 
                                warn!("cannot send frame directly because the destination is unknown, will drop it.");
                                continue;
                            };

                            self.swarm.behaviour_mut().oneway.send_request(&dest, frame);
                        },
                        Subscribe(topic) => {
                            if let Err(error) = self.swarm.behaviour_mut().pub_sub.subscribe(&topic) {
                                warn!(%error, %topic, "abonnement gossip échoué");
                            }
                        },
                        Publish{topic, payload} => {
                            if let Err(error) = self.swarm.behaviour_mut().pub_sub.publish(topic.hash(), payload) {
                                warn!(%error, %topic, "publication gossip échouée");
                            }                            
                        },
                        RegisterPeer { namespaces, bootstrap_peer_id, ttl } => {
                            for namespace in namespaces {
                                if let Err(error) = self.swarm.behaviour_mut().rendezvous.register(namespace, bootstrap_peer_id, ttl) {
                                    warn!(%error, "échec de l'enregistrement auprès du serveur bootstrap");
                                }
                            }

                        },
                        Shutdown => {
                            info!("arrêt du réseau (swarm libp2p) demandé");
                            break;
                        }
                    }

                },
                event = self.swarm.select_next_some() => {
                    match event {
                        Behaviour(Oneway(ReqResEvent::Message{peer, message: request_response::Message::Request{request: mut frame, ..}, ..})) => {
                            frame.source = Some(peer);
                            let _ = self.events_tx.send(NetworkEvent::ReceivedFrame(frame));
                        },
                        Behaviour(PubSub(msg)) => {
                            match msg {
                                gossipsub::Event::Message { propagation_source, message_id, message } => {
                                    let _ = self.events_tx.send(NetworkEvent::PubSubReceived { 
                                        id: message_id.to_string(),
                                        topic: message.topic.to_string(), 
                                        data: message.data, 
                                        source: propagation_source 
                                    });
                                },
                                _ => {}
                            }
                        },
                        Behaviour(Mdns(mdns::Event::Discovered(discovered))) => {
                            let non_connected = discovered
                                .into_iter()
                                .filter(|(peer_id, _addr)| !self.swarm.is_connected(&peer_id))
                                .collect::<Vec<_>>();

                            for (peer_id, addr) in non_connected {
                                if let Err(error) = self.swarm.dial(addr.clone()) {
                                    warn!(%peer_id, %addr, %error, "échec de connexion à un pair découvert par mDNS");
                                }
                            }
                        },
                        Behaviour(Mdns(mdns::Event::Expired(list))) => {
                            use NetworkEvent::PeerDisconnected;
                            for (peer_id, addr) in list {
                                let _ = self.events_tx.send(PeerDisconnected {peer_id});
                            }
                        },
                        Behaviour(Identify(IdEvent::Received { peer_id, info, .. })) => {
                            // On a trouvé le serveur de bootstrap (rendez-vous, etc.)
                            if info.agent_version.starts_with("/marie/bootstrap") {
                                use NetworkEvent::BootstrapDiscovered;
                                let _ = self.events_tx.send(BootstrapDiscovered{peer_id});
                            }
                            
                        },
                        Behaviour(Rendezvous(rendezvous::client::Event::Discovered {registrations, ..})) => {
                            for registration in registrations {
                                let peer_id = registration.record.peer_id();
                                if peer_id == *self.swarm.local_peer_id() { continue }

                                let ttl = registration.ttl; // ttl in seconds
                                let namespace = registration.namespace;
                                let _ = self.events_tx.send(NetworkEvent::NamespacePeerRegistred { peer_id, namespace, ttl });
                            }
                        },
                        SwarmEvent::ConnectionClosed { peer_id, num_established: 0, .. } => {
                            use NetworkEvent::PeerDisconnected;
                            let _ = self.events_tx.send(PeerDisconnected { peer_id });
                        },
                        _ => {}
                    }
                }

            }
        }
        
        let _ = self.shutdown_signal.send(true);

        Ok(())
    }
}