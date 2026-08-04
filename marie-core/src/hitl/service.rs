use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use std::collections::HashMap;
use thiserror::Error;
use typed_builder::TypedBuilder;

use crate::{
    di::{Constructible, Get, Resolve}, events::EventBus, hitl::{Answer, Hitl, HitlFrame, HitlId, model::{Answers, validate_answers}, protocol::{HitlRequest, HitlResponse}, store::HitlStore}, id::IdGenerator, session::{SessionId, protocol::SessionEvent}
};

/// Erreur qu'une opération de [`SessionHitls`]/[`HitlContainer`] peut
/// renvoyer — même idiome que `session::controller::SessionError`/
/// `CatalogError` : pas de `#[from]` sur [`crate::Error`], qui n'implémente
/// volontairement pas `std::error::Error` (voir sa doc), donc `thiserror` ne
/// peut pas en dériver `source()` automatiquement — conversion manuelle via
/// `.map_err(HitlError::StorageError)` sur chaque appel à [`HitlStore`].
#[derive(Debug, Error)]
pub enum HitlError {
    #[error("erreur lors des opérations de stockage: {0}")]
    StorageError(crate::Error),
    #[error("erreur de validation: {0}")]
    ValidationError(String),
}

#[derive(Clone)]
pub struct SessionHitlsFactory(Arc<dyn Fn(SessionId) -> SessionHitls + Send + Sync + 'static>);

impl SessionHitlsFactory {
    pub fn new<F>(factory: F) -> Self where F: Fn(SessionId) -> SessionHitls + Send + Sync + 'static {
        Self(Arc::new(factory))
    }

    pub fn create(&self, session_id: SessionId) -> SessionHitls {
        (self.0)(session_id)
    }
}

impl<C> Constructible<C> for SessionHitlsFactory 
    where C: Get<HitlStore> 
            + Resolve<EventBus>
            + Resolve<IdGenerator>
            + Clone + Send + Sync + 'static
{
    fn construct(container: &C, _: ()) -> Self {
        let container = container.clone();

        Self::new(move |session_id| {
            let args = SessionHitlsArgs::builder()
                .session_id(session_id)
                .id(container.resolve(()))
                .events(container.resolve(()))
                .store(container.get())
                .build();

            SessionHitls::new(args)
        })
    }
}

#[derive(TypedBuilder)]
pub struct SessionHitlsArgs {
    session_id: SessionId,
    id: IdGenerator,
    store: HitlStore,
    events: EventBus
}

/// Requêtes human-in-the-loop en cours d'une session — même rôle vis-à-vis
/// de [`HitlFrame`] que `session::snapshot::Snapshots` vis-à-vis de
/// `Snapshot` : cache de conteneurs adossé à Postgres (voir
/// `marie_session_hitls`, `migrations/0019_session_hitl.sql`), indexé par
/// [`HitlId`] — `session_id` est déjà fixé pour toute l'instance (une par
/// session, voir `session::controller::SessionHandler::hitls`), donc seule
/// la portion locale de la clé primaire `(session_id, id)` de la table sert
/// de clé de cache. `events` (voir [`crate::session::controller::
/// SessionController::events`]) est la même poignée que celle propagée à
/// [`crate::session::controller::SessionHandler`] — elle transitera par ici
/// pour notifier les abonnés d'une requête créée ou répondue.
#[derive(Clone)]
pub struct SessionHitls {
    id: IdGenerator,
    store: HitlStore,
    events: EventBus,
    session_id: SessionId
}

impl SessionHitls {
    pub fn new(args: SessionHitlsArgs) -> Self {
        Self {
            store: args.store,
            events: args.events,
            session_id: args.session_id,
            id: args.id,
        }
    }

    pub async fn reply(&self, id: HitlId, answers: HashMap<String, Answer>)  -> Result<HitlId, HitlError> {
        self.store
            .write_hitl_response(&self.session_id, &id, answers.clone())
            .await
            .map_err(HitlError::StorageError)?;

        let hitl = self.store.get_hitl(&self.session_id, &id).await.map_err(HitlError::StorageError)?;

        let answers = match hitl {
            Hitl::Questions(questions) => validate_answers(&questions, answers)?,
            Hitl::Text => Answers::from(answers)
        };

        let response = HitlResponse {
            session_id: self.session_id,
            id,
            answers,
        };

        self.events.emit(SessionEvent::HitlAnswered(response));

        Ok(id)
    }

    pub async fn request(&self, hitl: Hitl) -> Result<HitlId, HitlError> {
        let id = self.id.next();

        let frame = HitlFrame {
            session_id: self.session_id,
            id,
            hitl: hitl.clone(),
            answer: None,
        };

        let request = HitlRequest {
            session_id: self.session_id,
            id,
            hitl,
        };
        
        self.store.upsert_hitl_frame(frame).await.map_err(HitlError::StorageError)?;
        self.events.emit(SessionEvent::HitlRequested(request));

        Ok(id)
    }

}

pub struct HitlContainer {
    pub store: HitlStore,
    pub hitl: HitlFrame,
    pub dirty: bool
}

impl HitlContainer {
    /// Charge la requête `hitl_id` de la session `session_id` depuis `store`
    /// — point d'entrée unique de l'arène (voir `Hitls::hitls`) vers
    /// [`crate::session::store::StoreSession`], comme
    /// [`crate::session::snapshot::SnapshotContainer::new`].
    pub async fn new(store: HitlStore, session_id: SessionId, hitl_id: HitlId) -> crate::Result<Self> {
        let hitl = store.get_hitl_frame(&session_id, &hitl_id).await?;
        Ok(Self::from_loaded(store, hitl))
    }

    /// Enveloppe un [`HitlFrame`] déjà persisté (chargé par [`Self::new`])
    /// — `dirty` à `false`, contrairement à [`Self::from_hitl`].
    fn from_loaded(store: HitlStore, hitl: HitlFrame) -> Self {
        Self { store, hitl, dirty: false }
    }

    /// Enveloppe un [`HitlFrame`] fraîchement créé en mémoire, pas encore
    /// persisté (voir [`Hitls::push`]) — `dirty` à `true` d'emblée pour
    /// qu'il soit écrit au premier [`Self::flush`]/à son éviction du cache,
    /// comme
    /// [`crate::session::snapshot::SnapshotContainer::from_snapshot`].
    fn from_hitl(store: HitlStore, hitl: HitlFrame) -> Self {
        Self { store, hitl, dirty: true }
    }

    /// Persiste `hitl` si `dirty` est levé, puis rabaisse le drapeau — même
    /// logique que
    /// [`crate::session::snapshot::SnapshotContainer::flush`].
    pub async fn flush(&mut self) -> crate::Result<()> {
        if !self.dirty {
            return Ok(());
        }

        self.store.upsert_hitl_frame(self.hitl.clone()).await?;
        self.dirty = false;

        Ok(())
    }
}

impl Deref for HitlContainer {
    type Target = HitlFrame;

    fn deref(&self) -> &Self::Target {
        &self.hitl
    }
}

impl DerefMut for HitlContainer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        &mut self.hitl
    }
}

impl Drop for HitlContainer {
    /// `Drop` ne peut pas être `async` : à l'éviction du cache (voir
    /// `Hitls::hitls`), on détache un `tokio::spawn` fire-and-forget plutôt
    /// que de bloquer sur [`Self::flush`] — même logique que `Drop` pour
    /// `crate::session::snapshot::SnapshotContainer`.
    fn drop(&mut self) {
        if !self.dirty {
            return;
        }

        let store = self.store.clone();
        let hitl = self.hitl.clone();

        tokio::spawn(async move {
            if let Err(err) = store.upsert_hitl_frame(hitl).await {
                tracing::warn!("échec de la persistance d'une requête hitl évincée du cache: {err}");
            }
        });
    }
}