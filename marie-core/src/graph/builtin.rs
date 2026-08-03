
use marie_macros::core_node ;

#[cfg(feature = "node-executor")]
use crate::session::protocol::FrameResult;

#[cfg(feature="node-executor")]
use crate::graph::{node::NodeContext, script::GraphScriptEngine};

use crate::{graph::node::{Nodable, NodeSpec}, session::channel::Reducer};

core_node! {
    spec = {
        channels = {
            "script": {
                reducer: Reducer::LastWriteWins,
                inherited: true
            },
            "result": {
                reducer: Reducer::LastWriteWins,
                exported: true
            }
        }
    }

    async fn execute_script(self: Self<{script_engine: GraphScriptEngine}>, ctx: &mut NodeContext) -> crate::Result<FrameResult> {
        let script: String = ctx.read("script")?;
        let result = self.script_engine.execute(&script, ctx).await?;
        Ok(result)
    }
}

impl NodeSpec {
    /// [`NodeSpec`] prêt à l'emploi pour le node natif [`ExecuteScript`] —
    /// preset uniquement le canal `script` (le source Rune, voir la doc de
    /// [`crate::graph::script::GraphScriptEngine`]).
    pub fn execute_script(script: impl Into<String>) -> Self {
        let mut common = ExecuteScript::common_spec();
        common.overrides("script", script.into());

        Self {
            kind: ExecuteScript::kind_id(),
            common
        }
    }
}

core_node! {
    spec = {
        channels = {
            "task": {
                reducer: Reducer::LastWriteWins,
                inherited: true
            },
            "expert_id": {
                reducer: Reducer::LastWriteWins,
                inherited: true
            },
            "answer": {
                reducer: Reducer::LastWriteWins,
                exported: true
            },
        },
    }

    // Plus de canal `ask_id` fait maison pour détecter "ai-je déjà
    // demandé" : c'est exactement à quoi sert `ctx.logs` (voir
    // `session::run_log::RunLogs`), identité rejouable par position d'appel
    // plutôt que par un id qu'il fallait auparavant réécrire soi-même à
    // chaque rejeu (et que ce node ne faisait d'ailleurs jamais).
    async fn ask_expert(self: Self, ctx: &mut NodeContext) -> crate::Result<FrameResult> {
        use crate::expert::{ExpertId, RequestAskExpert};

        let expert_id: ExpertId = ctx.read("expert_id")?;
        let task: String = ctx.read("task")?;

        let (answer,): (String,) = crate::ask_experts!(ctx, RequestAskExpert { task, expert_id });

        ctx.write("answer", answer)?;
        Ok(FrameResult::Completed)
    }
}

impl NodeSpec {
    /// [`NodeSpec`] prêt à l'emploi pour le node natif [`AskExpert`] —
    /// preset `expert_id`/`task`. Contrairement à `answer`/`ask_id`
    /// (respectivement exporté et géré en interne par le node lui-même, voir
    /// son corps ci-dessus), ces deux canaux n'ont pas de valeur par défaut
    /// exploitable : ils doivent toujours être fixés pour cette instance.
    pub fn ask_expert(expert_id: impl Into<String>, task: impl Into<String>) -> Self {
        let mut common = AskExpert::common_spec();

        common.overrides("expert_id", expert_id.into());
        common.overrides("task", task.into());

        Self {
            kind: AskExpert::kind_id(),
            common
        }
    }
}