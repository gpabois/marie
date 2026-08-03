use std::{collections::HashMap, ops::{Deref, DerefMut}, sync::Arc};

use futures::{StreamExt, stream::BoxStream};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

use crate::{di::{Constructible, Get}, network::{Network, protocol::NetworkEvent}};

pub trait Event: DeserializeOwned + Serialize + Clone + Send + 'static {
    const TOPIC: &str;

    fn id(&self) -> String;
    fn topics(&self) -> Vec<String>;
}

pub trait RouteEvents {
    fn emit(&self, event: EventEnvelope);
    fn subscribe(&self, topic: &str);
    fn unsubscribe(&self, topic: &str);
    fn stream_events(&self, topic: &str) -> BoxStream<'static, EventEnvelope>;
}

#[derive(Clone)]
pub struct EventRouter(Arc<dyn RouteEvents + Send + Sync + 'static>);

impl EventRouter {
    pub fn new(router: impl RouteEvents + Send + Sync + 'static) -> Self {
        Self(Arc::new(router))
    }

    pub fn new_local() -> Self {
        Self::new(LocalEventRouter::new())
    }
}

impl RouteEvents for EventRouter {
    fn emit(&self, event: EventEnvelope) {
        self.0.emit(event);
    }

    fn subscribe(&self, topic: &str) {
        self.0.subscribe(topic);
    }

    fn unsubscribe(&self, topic: &str) {
        self.0.unsubscribe(topic);
    }

    fn stream_events(&self, topic: &str) -> BoxStream<'static, EventEnvelope> {
        self.0.stream_events(topic)
    }
}

/// Implémentation de [`RouteEvents`] adossée au réseau libp2p (voir
/// [`crate::network::Network`]) — `emit`/`subscribe`/`unsubscribe` relaient
/// directement vers gossipsub (voir `NetworkStrategy::emit_event`/
/// `subscribe`/`unsubscribe`), `stream_events` filtre le flux `NetworkEvent`
/// partagé du nœud (chaque appel obtient son propre récepteur `broadcast`,
/// voir `SwarmNetwork::stream_events`) pour ne garder que les
/// `NetworkEvent::EventReceived` du topic demandé : les autres variantes
/// (connexions, découverte bootstrap, etc.) ne concernent pas le routing
/// d'évènements applicatifs.
///
/// Embarque aussi un [`LocalEventRouter`] : gossipsub ne redélivre jamais à
/// l'émetteur ses propres messages, donc sans lui un [`Self::emit`] resterait
/// invisible aux abonnés locaux (même nœud, même processus) tant qu'aucun
/// pair distant ne l'a renvoyé. `Self::emit`/`subscribe`/`unsubscribe`
/// propagent donc systématiquement vers les deux, et `Self::stream_events`
/// fusionne le flux local avec le flux réseau — sans risque de doublon
/// puisque le second ne contient jamais les évènements du premier.
#[derive(Clone)]
pub struct NetworkEventRouter {
    network: Network,
    local: LocalEventRouter,
}

impl NetworkEventRouter {
    pub fn new(network: Network) -> Self {
        Self { network, local: LocalEventRouter::new() }
    }
}

impl RouteEvents for NetworkEventRouter {
    fn emit(&self, event: EventEnvelope) {
        self.local.emit(event.clone());
        self.network.emit_event(event);
    }

    fn subscribe(&self, topic: &str) {
        self.local.subscribe(topic);
        self.network.subscribe(topic);
    }

    fn unsubscribe(&self, topic: &str) {
        self.local.unsubscribe(topic);
        self.network.unsubscribe(topic);
    }

    fn stream_events(&self, topic: &str) -> BoxStream<'static, EventEnvelope> {
        let owned_topic = topic.to_string();

        let network_stream = self.network
            .stream_events()
            .filter_map(move |event| {
                let matched = match event {
                    NetworkEvent::EventReceived { id, topic: event_topic, data, .. } if event_topic == owned_topic => {
                        Some(EventEnvelope { id, topic: event_topic, payload: data })
                    }
                    _ => None,
                };
                std::future::ready(matched)
            });

        let local_stream = self.local.stream_events(topic);

        futures::stream::select(local_stream, network_stream).boxed()
    }
}

/// Implémentation de [`RouteEvents`] purement locale (un seul processus, pas
/// de réseau) — un [`tokio::sync::broadcast`] par topic plutôt qu'un unique
/// canal partagé : `stream_events(topic)` ne doit recevoir que les
/// évènements de son topic, et un topic jamais souscrit ne doit pas faire
/// grossir [`Self::topics`] indéfiniment à chaque `emit` (voir [`Self::emit`],
/// qui n'envoie que si un canal existe déjà — donc si quelqu'un a déjà
/// souscrit). `Self::channel` sert à la fois à [`Self::subscribe`] (créer le
/// canal avant qu'un abonné n'arrive) et à [`Self::stream_events`] (get-or-
/// create défensif : `EventService::stream_events` construit le flux avant
/// d'appeler `subscribe`, voir sa doc).
#[derive(Clone)]
pub struct LocalEventRouter {
    topics: Arc<Mutex<HashMap<String, broadcast::Sender<EventEnvelope>>>>,
}

impl LocalEventRouter {
    const CHANNEL_CAPACITY: usize = 256;

    pub fn new() -> Self {
        Self { topics: Arc::new(Mutex::new(HashMap::new())) }
    }

    fn channel(&self, topic: &str) -> broadcast::Sender<EventEnvelope> {
        self.topics
            .lock()
            .entry(topic.to_string())
            .or_insert_with(|| broadcast::channel(Self::CHANNEL_CAPACITY).0)
            .clone()
    }
}

impl Default for LocalEventRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl RouteEvents for LocalEventRouter {
    fn emit(&self, event: EventEnvelope) {
        if let Some(tx) = self.topics.lock().get(&event.topic) {
            // Erreur si personne n'écoute actuellement ce canal : sans
            // conséquence, un topic souscrit plus tard repart d'un flux vide
            // plutôt que d'un historique — même sémantique que gossipsub côté
            // `NetworkEventRouter`.
            let _ = tx.send(event);
        }
    }

    fn subscribe(&self, topic: &str) {
        self.channel(topic);
    }

    fn unsubscribe(&self, topic: &str) {
        self.topics.lock().remove(topic);
    }

    fn stream_events(&self, topic: &str) -> BoxStream<'static, EventEnvelope> {
        let rx = self.channel(topic).subscribe();

        BroadcastStream::new(rx)
            .filter_map(|result| std::future::ready(result.ok()))
            .boxed()
    }
}

pub struct EventSubscription<E: Event> {
    topic: String,
    service: EventBus,
    stream: BoxStream<'static, EventEnvelope<E>>
}

impl<E: Event> EventSubscription<E> {
    pub fn new(topic: String, service: EventBus, stream: BoxStream<'static, EventEnvelope<E>>) -> Self {
        service.inc_sub(topic.clone());
        Self {
            topic,
            service,
            stream
        }
    }
}

impl<E: Event> Deref for EventSubscription<E> {
    type Target = BoxStream<'static, EventEnvelope<E>>;

    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

impl<E: Event> DerefMut for EventSubscription<E> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream
    }
}


#[derive(Clone)]
pub struct EventBus {
    router: EventRouter,
    counter: Arc<Mutex<HashMap<String, u32>>>
}

impl<C> Constructible<C> for EventBus where C: Get<EventRouter> {
    fn construct(container: &C, _: ()) -> Self {
        Self::new(container.get())
    }
}

impl EventBus {
    fn new(router: EventRouter) -> Self {
        Self {
            router,
            counter: Arc::new(Mutex::new(HashMap::default()))
        }
    }
    pub fn local() -> EventBus {
        Self::new(EventRouter::new(LocalEventRouter::new()))
    }

    pub fn network(network: Network) -> EventBus {
        Self::new(EventRouter::new(NetworkEventRouter::new(network)))
    }
}

impl EventBus {
    pub fn emit<E: Event>(&self, event: E) {
        event.topics()
            .into_iter()
            .for_each(|topic| self.router.emit(EventEnvelope { id: event.id().to_string(), topic, payload: serde_json::to_vec(&event).unwrap() }));
       
    }

    pub fn stream_events<E: Event>(&self, topic: impl ToString) -> EventSubscription<E> {
        let topic = topic.to_string();

        let stream = {
            let topic = topic.clone();
            self.router
            .stream_events(&topic)
            .filter_map(move |env| {
                if env.topic != topic { return std::future::ready(None) }
                match env.deserialize() {
                    Ok(env) => { std::future::ready(Some(env.payload))},
                    Err(_) => { return std::future::ready(None) }
                }
            }).boxed()
        };

        EventSubscription::new(topic.clone(), self.clone(), stream)
    }
}

impl EventBus {
    fn inc_sub(&self, topic: impl ToString) {
        let topic = topic.to_string();
        let mut guard = self.counter.lock();
        let counter: &mut u32 = guard.entry(topic.clone()).or_default();
        *counter += 1;

        if *counter == 1 {
            self.router.subscribe(&topic)
        }
    }

    fn dec_sub(&self, topic: impl ToString) {
        let topic = topic.to_string();
        let mut guard = self.counter.lock();
        let counter: &mut u32 = guard.entry(topic.clone()).or_default();
        let value = counter.checked_sub(1);
        
        if value.is_none() || Some(0) == value {
            self.router.unsubscribe(&topic)
        }
    }
    
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EventEnvelope<T=Vec<u8>> {
    pub id: String,
    pub topic: String,
    pub payload: T,
}

impl<T> EventEnvelope<T> {
    pub fn new(id: impl ToString, topic: impl ToString, payload: T) -> Self {
        Self {
            id: id.to_string(),
            topic: topic.to_string(),
            payload
        }
    }
}

impl<T: Serialize> EventEnvelope<T> {
    pub fn serialize(self) -> crate::Result<EventEnvelope> {
        Ok(EventEnvelope {
            id: self.id,
            topic: self.topic,
            payload: serde_json::to_vec(&self.payload)?
        })
    }
}

impl EventEnvelope {
    pub fn deserialize<T: DeserializeOwned>(self) -> crate::Result<EventEnvelope<T>> {
        Ok(EventEnvelope {
            id: self.id,
            topic: self.topic,
            payload: serde_json::from_slice(&self.payload)?,
        })
    }
}

enum Command {
    Subscribe(String),
    Unsubscribe(String)
}

pub struct PubSub;