use async_trait::async_trait;
use sqlx::{Row as _, postgres::PgRow, types::Json};

use crate::{
    store::PgStore,
    expert::{Expert, ExpertId}, 
    model::ModelId, 
    tools::ToolId
};

#[async_trait]
impl super::ExpertStorable for PgStore {
    async fn get(&self, id: ExpertId) -> crate::Result<Option<Expert>> {
        let id = id.to_string();
        let row = sqlx::query("SELECT id, prompt, model_id, allowed_tools FROM marie_experts WHERE id = $1")
            .bind(&id)
            .fetch_optional(self.pool())
            .await?;
        row.as_ref().map(decode_row).transpose()
    }

    async fn insert(&self, value: Expert) -> crate::Result<()> {
        let id = value.id.to_string();
        let model_id = value.model_id.to_string();

        sqlx::query("INSERT INTO marie_experts (id, prompt, model_id, allowed_tools) VALUES ($1, $2, $3, $4)")
            .bind(&id)
            .bind(&value.prompt)
            .bind(&model_id)
            .bind(Json(&value.allowed_tools))
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn replace(&self, value: Expert) -> crate::Result<()> {
        let id = value.id.to_string();
        let model_id = value.model_id.to_string();

        sqlx::query("UPDATE marie_experts SET prompt = $2, model_id = $3, allowed_tools = $4 WHERE id = $1")
            .bind(&id)
            .bind(&value.prompt)
            .bind(&model_id)
            .bind(Json(&value.allowed_tools))
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn delete(&self, id: ExpertId) -> crate::Result<()> {
        let id = id.to_string();
        sqlx::query("DELETE FROM marie_experts WHERE id = $1").bind(&id).execute(self.pool()).await?;
        Ok(())
    }

    async fn list(&self) -> crate::Result<Vec<Expert>> {
        let rows = sqlx::query("SELECT id, prompt, model_id, allowed_tools FROM marie_experts").fetch_all(self.pool()).await?;
        rows.iter().map(decode_row).collect()
    }
}

/// Reconstitue un [`Expert`] depuis une ligne de la table `expert` (voir
/// `migrations/0003_expert.sql`) — symétrique de l'insertion dans
/// [`PgStore::insert`]/[`PgStore::replace`].
fn decode_row(row: &PgRow) -> crate::Result<Expert> {
    Ok(Expert {
        id: ExpertId::new(row.try_get::<String, _>("id")?),
        prompt: row.try_get("prompt")?,
        model_id: ModelId::new(row.try_get::<String, _>("model_id")?),
        allowed_tools: row.try_get::<Json<Vec<ToolId>>, _>("allowed_tools")?.0,
    })
}


