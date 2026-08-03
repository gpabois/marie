use typed_builder::TypedBuilder;

use crate::{events::EventBus, id::IdGenerator, session::{logs::store::SessionLogStore, model::SessionId, protocol::SessionEvent}};

pub mod store;
pub mod model;
pub mod factory;

pub use factory::SessionLogsFactory;
pub use model::{SessionLog, SessionLogContent, SessionLogId};

#[derive(TypedBuilder)]
pub struct SessionLogsArgs {
    session_id: SessionId,
    store: SessionLogStore,
    id: IdGenerator,
    bus: EventBus
}

#[derive(Clone)]
/// Journal d'une session — même rôle vis-à-vis de [`SessionLog`] que
/// `session::hitls::Hitls` vis-à-vis de `HitlFrame` : une instance par
/// session (`session_id` fixé une fois pour toutes), qui persiste dans
/// `store` et notifie `bus` (voir [`SessionEvent::LogCreated`]/
/// [`SessionEvent::LogUpdated`]) à chaque écriture. Contrairement à
/// `Hitls`/`FrameTree`, pas de cache de conteneurs `dirty` : une entrée de
/// journal est petite et chaque écriture (initiale ou en remplacement, voir
/// [`Self::replace_log`]) est immédiatement persistée via
/// [`SessionStore::insert_log`], qui fait déjà l'upsert.
pub struct SessionLogs {
    session_id: SessionId,
    store: SessionLogStore,
    id: IdGenerator,
    bus: EventBus,
}

impl SessionLogs {
    pub fn new(args: SessionLogsArgs) -> Self {
        Self { 
            id: args.id,
            store: args.store, 
            bus: args.bus,
            session_id: args.session_id 
        }
    }

    /// Ajoute une nouvelle entrée au journal, avec un [`SessionLogId`] neuf —
    /// pour y ajouter du texte au fil de l'eau par la suite, voir
    /// [`Self::replace_log`] plutôt que rappeler [`Self::log`].
    pub async fn log(&self, content: SessionLogContent) -> crate::Result<SessionLogId> {
        let id = self.id.next();
        let now = chrono::Utc::now();

        let log = SessionLog {
            id,
            session_id: self.session_id,
            content,
            created_at: now,
            last_updated_at: now,
        };

        self.store.insert_log(log.clone()).await?;
        self.bus.emit(SessionEvent::LogCreated(log));

        Ok(id)
    }

    /// Remplace le contenu de l'entrée `id`, déjà créée par un appel
    /// précédent à [`Self::log`]. `created_at` n'est ici qu'une valeur de
    /// repli utilisée si `id` n'existe pas encore côté `store` : sur un
    /// conflit, [`SessionStore::insert_log`] ne touche que `content`/
    /// `last_updated_at`, la date de création d'origine est donc préservée.
    pub async fn replace_log(&self, id: SessionLogId, content: SessionLogContent) -> crate::Result<()> {
        let now = chrono::Utc::now();

        let log = SessionLog {
            id,
            session_id: self.session_id,
            content,
            created_at: now,
            last_updated_at: now,
        };

        self.store.insert_log(log.clone()).await?;
        self.bus.emit(SessionEvent::LogUpdated(log));

        Ok(())
    }
}