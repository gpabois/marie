pub mod model;
pub use model::{ExpertId, Expert, ExpertAskId};
use futures::{StreamExt as _, pin_mut};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use std::borrow::Borrow;

use crate::{
    agent::{Context, ContextEntry, Role}, catalog::{Catalog, CatalogError, CatalogItemRef}, di::{Constructible, Resolve},
    model::{ExecuteModelArgs, ModelError, ModelResponse, ModelStreamEvent, Models}, tools::Tools
};

#[derive(Debug, Error)]
pub enum ExpertError {
    #[error("erreur de catalogue: {0}")]
    CatalogError(#[from] CatalogError),
    #[error("expert introuvable: {0}")]
    NotFound(ExpertId),
    #[error("erreur du modèle: {0}")]
    ModelError(#[from] ModelError),
    #[error("échec de la consultation: {0}")]
    ConsultationFailed(String),
}

#[derive(Clone)]
pub struct Experts {
    catalog: Catalog,
    models: Models,
    tools: Tools,
}

impl<C> Constructible<C> for Experts where C: Resolve<Catalog> + Resolve<Models> + Resolve<Tools> {
    fn construct(container: &C, _: ()) -> Self {
        Self::new(container.resolve(()), container.resolve(()), container.resolve(()))
    }
}

impl Experts {
    pub fn new(catalog: Catalog, models: Models, tools: Tools) -> Self {
        Self { catalog, models, tools }
    }

    /// Consultation synchrone d'un expert — contourne entièrement la
    /// machinerie session/graphe (voir [`crate::session::checkpointer`]) :
    /// résout `id` en [`Expert`], résout ses `allowed_tools` en
    /// [`crate::tools::Tool`]s concrets, construit un contexte
    /// system/user à partir de `expert.prompt`/`task`, puis délègue à
    /// [`Models::execute`]. Draine le flux jusqu'à
    /// [`ModelStreamEvent::Completed`] plutôt que de le relayer
    /// incrémentalement : contrairement à une session (qui journalise
    /// chaque [`ModelStreamEvent::TextDelta`] au fil de l'eau via
    /// `SessionClient::insert_in_log`), un appelant de `consult` n'a rien
    /// à quoi rattacher un flux partiel — voir `Experts::consult` côté CLI
    /// (`consult expert`), seul appelant à ce jour.
    pub async fn consult(&self, id: &ExpertId, task: impl ToString) -> Result<ModelResponse, ExpertError> {
        let expert = self.get(id).await?.ok_or_else(|| ExpertError::NotFound(id.clone()))?;

        let mut tools = Vec::with_capacity(expert.allowed_tools.len());
        for tool_id in &expert.allowed_tools {
            if let Some(tool) = self.tools.get(tool_id).await? {
                tools.push(tool);
            }
        }

        let context = Context::from(vec![
            ContextEntry { role: Role::System, content: expert.prompt.clone() },
            ContextEntry { role: Role::User, content: task.to_string() },
        ]);

        let args = ExecuteModelArgs::builder()
            .model(expert.model_id.clone())
            .context(context)
            .tools(tools)
            .build();

        let stream = self.models.execute(args).await?;
        pin_mut!(stream);

        while let Some(event) = stream.next().await {
            match event {
                ModelStreamEvent::Completed(response) => return Ok(response),
                ModelStreamEvent::Failed(message) => return Err(ExpertError::ConsultationFailed(message)),
                ModelStreamEvent::TextDelta(_) => {}
            }
        }

        Err(ExpertError::ConsultationFailed("le flux du modèle s'est terminé sans réponse".to_string()))
    }

    pub async fn create(&self, expert: Expert) -> Result<CatalogItemRef, CatalogError> {
        self.catalog.publish(expert).await
    }

    /// Version active la plus récente de `id`, désérialisée — sur le même
    /// modèle que [`crate::model::Models::latest`]/[`crate::tools::Tools::get`].
    pub async fn get(&self, id: &ExpertId) -> Result<Option<Expert>, CatalogError> {
        match self.catalog.latest::<Expert>(id.borrow()).await {
            Ok(item) => Ok(Some((*item).clone())),
            Err(CatalogError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Soft-delete (voir [`Catalog::delete`]) : retire l'expert `id` du
    /// catalogue actif, en le résolvant d'abord à sa version active la plus
    /// récente — échoue avec [`CatalogError::NotFound`] si `id` n'existe pas
    /// déjà.
    pub async fn delete(&self, id: &ExpertId) -> Result<(), CatalogError> {
        let r = self.catalog.latest_ref::<Expert>(id.borrow()).await?;
        self.catalog.delete(&r).await
    }

    pub async fn list(&self) -> Result<Vec<Expert>, CatalogError> {
        let refs = self.catalog.list::<Expert>().await?;
        let mut experts = Vec::with_capacity(refs.len());
        for r in refs {
            let item = self.catalog.deref::<Expert>(&r).await?;
            experts.push((*item).clone());
        }
        Ok(experts)
    }

    /// Publie une nouvelle version d'un expert déjà connu — contrairement à
    /// [`Self::create`], échoue avec [`CatalogError::NotFound`] si
    /// `expert.id` n'a encore aucune version active (pas de remplacement à
    /// faire).
    pub async fn replace(&self, expert: Expert) -> Result<CatalogItemRef, CatalogError> {
        self.catalog.latest_ref::<Expert>(expert.id.borrow()).await?;
        self.catalog.publish(expert).await
    }
}

/// Ce qu'un corps de node/script construit pour demander l'avis d'un expert
/// (voir `ask_experts!`/le Rune `ask_experts(...)`) — volontairement
/// dépourvu d'identifiant : le déterminisme du rejeu (voir
/// [`crate::session::run_log::RunLogs`]) interdit qu'un appelant en génère
/// un lui-même (aléatoire par nature, donc différent à chaque rejeu).
/// `SessionHandler::append_expert_asking` la transforme en [`AskExpert`] en
/// lui attachant un [`ExpertAskId`] fraîchement généré, côté serveur — même
/// principe que [`crate::hitl::Hitl`] transformé en
/// [`crate::hitl::protocol::HitlRequest`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestAskExpert {
    pub task: String,
    pub expert_id: ExpertId
}

/// Version de [`RequestAskExpert`] portant l'[`ExpertAskId`] généré côté
/// `SessionHandler` — voir sa doc.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AskExpert {
    pub id: ExpertAskId,
    pub task: String,
    pub expert_id: ExpertId
}