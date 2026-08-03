
use std::collections::HashMap;

pub use marie_macros::core_node;

use async_trait::async_trait;
#[cfg(feature = "node-executor")]
use futures::FutureExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

#[cfg(feature = "node-executor")]
use crate::session::protocol::FrameResult;
use crate::{
    graph::Graphs, session::{SessionId, channel::{ChannelName, ChannelUpdate}, frames::FrameId, run_log::RunLogs, spec::CommonSpec}
};


#[derive(Clone, Serialize, Deserialize)]
pub struct NodeSpec {
    pub kind: NodeKindId,
    pub common: CommonSpec,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(String);

impl From<&str> for NodeId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for NodeId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// Identifiant d'un *type* de node native ([`Nodable::NAME`]) — distinct de
/// [`NodeId`], qui identifie une node *instanciée* dans un
/// [`crate::graph::GraphSpec`] (`GraphSpec::nodes: HashMap<NodeId,
/// NodeSpec>`). Sert de clé au registre en mémoire tenu par [`Graphs`] (voir
/// [`Graphs::register`]), sur le même principe que [`crate::tools::ToolId`]
/// pour [`crate::tools::Toolable`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeKindId(String);

impl From<&str> for NodeKindId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for NodeKindId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// Contexte d'exécution d'une node native — `channels` est l'état hérité,
/// chargé une fois depuis le [`crate::session::snapshot::Snapshot`] lié au
/// `NodeFrame` en cours d'exécution (voir [`Self::new`]), jamais réécrit
/// directement par [`Self::write`] : chaque écriture s'accumule plutôt dans
/// `updates` sous forme de [`ChannelUpdate`] (voir sa doc), pour que
/// l'appelant (le driver de job, `session::worker::run_frame`) puisse les
/// reporter telles quelles dans [`crate::session::protocol::FrameResponse::updates`]
/// et laisser le réducteur de chaque canal (voir
/// [`crate::session::channel::Reducer`]) les combiner au prochain superstep —
/// `NodeContext` lui-même ne connaît pas les réducteurs déclarés (portés par
/// [`CommonSpec`], pas par `NodeContext`).
#[derive(Clone)]
pub struct NodeContext {
    pub session_id: SessionId,
    /// Frame en cours d'exécution — porté comme [`ChannelUpdate::contributor`]
    /// de chaque écriture, pour que le réducteur sache quelle contribution
    /// vient d'où.
    pub frame_id: FrameId,
    pub(crate) channels: HashMap<ChannelName, Value>,
    pub updates: Vec<ChannelUpdate>,
    /// Journal de rejeu déterministe de ce frame — voir
    /// [`crate::session::run_log::RunLogs`] et les macros `ask_experts!`/
    /// `call_tools!`/`hitl!`/`graph!` qui l'utilisent.
    pub logs: RunLogs,
}

impl NodeContext {
    /// `channels` : état hérité, tel que lu depuis le `Snapshot` du
    /// `NodeFrame` (voir la doc de [`Self`]) — les surcharges de
    /// [`CommonSpec::overrides_channels`] sont appliquées par
    /// `SessionHandler` sur ces canaux hérités avant même la construction du
    /// `NodeContext`, donc déjà présentes ici.
    pub fn new(session_id: SessionId, frame_id: FrameId, channels: HashMap<ChannelName, Value>, logs: RunLogs) -> Self {
        Self { session_id, frame_id, channels, updates: Vec::new(), logs }
    }

    /// N'écrit jamais directement dans `channels` : empile un
    /// [`ChannelUpdate`] dans [`Self::updates`], visible aux lectures
    /// suivantes de ce même canal (voir [`Self::read`]) sans attendre que le
    /// réducteur du canal l'ait combiné à l'état hérité.
    pub fn write(&mut self, name: impl Into<ChannelName>, value: impl Serialize) -> crate::Result<()> {
        let name = name.into();
        let value = serde_json::to_value(value).unwrap();
        self.updates.push(ChannelUpdate { name, value, contributor: self.frame_id });
        Ok(())
    }

    /// Le [`ChannelUpdate`] le plus récent déjà écrit pour `name` au cours de
    /// cette exécution (retire-écriture immédiate, avant tout passage par un
    /// réducteur) — sinon la valeur héritée du `Snapshot` (voir la doc de
    /// [`Self`]).
    pub fn read<V: DeserializeOwned>(&self, name: impl Into<ChannelName>) -> crate::Result<V> {
        let name = name.into();

        let value = self.updates.iter().rev()
            .find(|update| update.name == name)
            .map(|update| &update.value)
            .or_else(|| self.channels.get(&name))
            .cloned()
            .ok_or_else(|| crate::err!("no channel {name} exists"))?;

        let value = serde_json::from_value(value)?;
        Ok(value)
    }
}

/// Node native — sur le même principe que [`crate::tools::Toolable`] : un
/// type Rust qui décrit et (avec la feature `node-executor`) exécute
/// lui-même une node, plutôt qu'une entrée versionnée du catalogue. Jamais
/// écrit dans un [`crate::graph::GraphSpec`] (qui ne référence les nodes
/// natives que par [`NodeId`]) ; [`Self::register`] l'ajoute au registre en
/// mémoire tenu par [`Graphs`] (voir [`Graphs::register`]).
#[async_trait]
pub trait Nodable: Sized + Clone + Send + Sync + 'static {
    const NAME: &str;

    /// Retourne la spécification native de cette node.
    fn common_spec() -> CommonSpec;

    fn kind_id() -> NodeKindId {
        NodeKindId::from(Self::NAME)
    }

    #[cfg(feature = "node-executor")]
    async fn execute(self, ctx: &mut NodeContext) -> crate::Result<FrameResult>;

    #[cfg(not(feature = "node-executor"))]
    fn register(self, graphs: &Graphs) {
        graphs.register(Self::kind_id(), Self::common_spec());
    }

    #[cfg(feature = "node-executor")]
    fn register(self, graphs: &Graphs) {
        let executor = move |mut ctx: NodeContext| {
            let this = self.clone();
            async move {
                let response = this.execute(&mut ctx).await?;
                Ok((ctx, response))
            }.boxed()
        };

        graphs.register(Self::kind_id(), Self::common_spec(), executor);
    }
}
