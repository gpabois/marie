use futures::{StreamExt, stream::BoxStream};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::network::{Network, protocol::NetworkEvent};

pub trait Event: DeserializeOwned + Serialize + Clone + Send + 'static {
    const TOPIC: &str;

    fn id(&self) -> String;
    fn topics(&self) -> Vec<String>;
}

#[derive(Clone)]
pub struct EventService {
    network: Network
}

impl EventService {
    pub fn emit_event<E: Event>(&self, event: E) {
        event.topics()
            .into_iter()
            .for_each(|topic| self.network.emit_event(EventEnvelope { id: event.id().to_string(), topic, payload: serde_json::to_vec(&event).unwrap() }));
       
    }

    pub fn subscribe(&self, topic: &str) {
        self.network.subscribe(topic);
    }

    pub fn stream_events<E: Event>(&self, topic: impl ToString) -> BoxStream<E> {
        let topic = topic.to_string();

        self.network
            .stream_events()
            .filter_map(|ev| {
                if let NetworkEvent::EventReceived { id, topic, data, source } = ev {
                    std::future::ready(Some(EventEnvelope {
                        id,
                        topic,
                        payload: data
                    }))
                } else {
                    std::future::ready(None)
                }
            })
            .filter_map(move |env| {
                if env.topic != topic { return std::future::ready(None) }
                match env.deserialize() {
                    Ok(env) => { std::future::ready(Some(env.payload))},
                    Err(_) => { return std::future::ready(None) }
                }
            }).boxed()
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