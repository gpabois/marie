use futures::stream::{BoxStream, Stream, StreamExt, SelectAll};
use tokio::sync::mpsc;
use tokio_stream::wrappers::IntervalStream;

/// Handle clonable permettant d'injecter de nouveaux flux depuis n'importe où.
pub struct StreamHandle<T> {
    tx: mpsc::Sender<BoxStream<'static, T>>,
}

impl<T> Clone for StreamHandle<T> {
    fn clone(&self) -> Self {
        Self { tx: self.tx.clone() }
    }
}

impl<T: Clone + Send + 'static> StreamHandle<T> {
    /// Envoie un nouveau flux dans le pool (version async).
    pub async fn push<S>(&self, stream: S) -> Result<(), mpsc::error::SendError<BoxStream<'static, T>>>
    where
        S: Stream<Item = T> + Send + 'static,
    {
        self.tx.send(stream.boxed()).await
    }

    /// Envoie un flux de manière synchrone/non-bloquante.
    pub fn try_push<S>(&self, stream: S) -> Result<(), mpsc::error::TrySendError<BoxStream<'static, T>>>
    where
        S: Stream<Item = T> + Send + 'static,
    {
        self.tx.try_send(stream.boxed())
    }

    pub async fn after(&self, duration: chrono::Duration, value: T) {
        let interval = tokio::time::interval(duration.to_std().unwrap());
        let stream = IntervalStream::new(interval).map(move |_| value.clone());
        let _ = self.push(stream).await;
    }
}

/// Gestionnaire principal qui agrège et poll l'ensemble des flux dynamiques.
pub struct DynamicStreamPool<T> {
    select_all: SelectAll<BoxStream<'static, T>>,
    rx: mpsc::Receiver<BoxStream<'static, T>>,
    tx: mpsc::Sender<BoxStream<'static, T>>,
}

impl<T: 'static> DynamicStreamPool<T> {
    /// Crée un nouveau pool avec une capacité de canal donnée.
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self {
            select_all: SelectAll::new(),
            rx,
            tx,
        }
    }

    /// Récupère un nouveau `StreamHandle` clonable pour ajouter des flux au pool.
    pub fn handle(&self) -> StreamHandle<T> {
        StreamHandle {
            tx: self.tx.clone(),
        }
    }

    /// Ajoute directement un flux si vous avez un accès mutable local au pool.
    pub fn push<S>(&mut self, stream: S)
    where
        S: Stream<Item = T> + Send + 'static,
    {
        self.select_all.push(stream.boxed());
    }

    /// Récupère le prochain élément disponible dans l'ensemble des flux enregistrés.
    /// Renvoie `None` uniquement lorsque tous les handles sont détruits ET que tous les flux sont terminés.
    pub async fn next(&mut self) -> Option<T> {
        loop {
            tokio::select! {
                // 1. Réception et enregistrement d'un nouveau flux
                Some(new_stream) = self.rx.recv() => {
                    self.select_all.push(new_stream);
                }

                // 2. Depilage du prochain élément si au moins un flux est actif
                Some(item) = self.select_all.next(), if !self.select_all.is_empty() => {
                    return Some(item);
                }

                // 3. Gestion des états vides ou de fermeture
                else => {
                    if self.select_all.is_empty() {
                        // Le pool est vide : on attend impatiemment le prochain flux.
                        // Si rx.recv() renvoie None, cela signifie que tous les handles ont été drop.
                        match self.rx.recv().await {
                            Some(new_stream) => self.select_all.push(new_stream),
                            None => return None, // Fin définitive du pool
                        }
                    }
                }
            }
        }
    }
}