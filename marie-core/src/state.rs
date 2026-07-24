use anyhow::anyhow;
use json_patch::Patch;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{graph::GraphFrameId, schema::Schema, session::SessionId, workspace::WorkspaceId};

pub trait StateAccess {
    fn query(&mut self, path: &str) -> anyhow::Result<Vec<Value>>;
    fn patch(&mut self, patch: Patch) -> anyhow::Result<()>;

    fn patch_from_value(&mut self, patch: Value) -> anyhow::Result<()> {
        let patch: Patch = serde_json::from_value(patch)?;
        self.patch(patch)
    }
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag="kind")]
pub enum StateLocation {
    InSession(SessionId),
    InWorkspace(WorkspaceId),
    InGraph(GraphFrameId)
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct State {
    pub instance: Value,
    pub schema: Option<Schema>
}

impl StateAccess for State {
    fn query(&self, path: &str) -> anyhow::Result<Vec<Value>> {
        let query = jsonpath_rust::query::js_path_vals(path, &self.instance)?;
        Ok(query.into_iter().cloned().collect())
    }

    fn patch(&mut self, patch: Patch) -> anyhow::Result<()> {
        if let Some(schema) = &self.schema {
            let mut instance = self.instance.clone();
            json_patch::patch(&mut instance, &patch);
            jsonschema::validate(&schema, &instance)
                .map_err(|err| anyhow!("state structure violation while patching : {err:?}"))?;
            self.instance = instance;
            Ok(())
        } else {
            json_patch::patch(&mut self.instance, &patch);
            Ok(())
        }
    }
}
pub struct StateTransaction {
    pub instance: State,
    pub patches: Vec<Patch>
}

impl From<StateTransaction> for Vec<Patch> {
    fn from(value: StateTransaction) -> Self {
        value.patches
    }
}

impl StateAccess for StateTransaction {
    fn query(&mut self, path: &str) -> anyhow::Result<Vec<Value>> {
        self.instance.query(path)
    }

    fn patch(&mut self, patch: Patch) -> anyhow::Result<()> {
        self.instance.patch(patch.clone())?;
        self.patches.push(patch);
        Ok(())
    }
}

impl StateTransaction {
    pub fn new(instance: State) -> Self {
        Self {
            instance,
            patches: vec![]
        }
    }
}