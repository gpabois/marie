use futures::{StreamExt as _, pin_mut};
use marie_macros::core_job;
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

#[cfg(feature="job-executor")]
use crate::{expert::ExpertId, session::channel::ChannelsTracker};
use crate::{
    session::channel::Channels,
    agent::{Context, ContextEntry, Role}, expert::Experts, graph::{Graphs, node::NodeContext}, model::{ExecuteModelArgs, ModelStreamEvent, Models}, session::{
        SessionId, channel::{ChannelName, ChannelUpdate}, frames::{FrameId, FrameKind}, protocol::{FrameResponse, FrameResult}, run_log::{RunLog, RunLogs}
    }, tools::{ToolId, Tools}
};

#[derive(Debug, Clone, Serialize, Deserialize, TypedBuilder)]
pub struct RunFrameArgs {
    session_id: SessionId,
    frame_id: FrameId,
    /// Canaux hérités du dernier cliché du `NodeFrame` — fournis par
    /// l'appelant (qui a déjà la session en main, voir
    /// `session::controller::SessionHandler`) plutôt que relus ici depuis
    /// `SessionStore` : un job est une unité de calcul bornée, elle ne doit
    /// pas avoir besoin d'un accès direct au stockage pour s'exécuter.
    channels: Channels,
    /// Journal de rejeu déterministe du `NodeFrame` — même principe que
    /// `channels` ci-dessus (fourni par l'appelant, pas relu ici) : voir
    /// `session::run_log::RunLogs`.
    logs: Vec<RunLog>,
    data: FrameKind
}

core_job! {
    #[job(name="/marie/sessions/jobs/run-frame")]
    pub async fn run_frame(self: Self<{models: Models, tools: Tools, graphs: Graphs, experts: Experts}>, args: RunFrameArgs) -> FrameResponse {
        let mut channels = ChannelsTracker::new(
            args.frame_id,
            args.channels
        );

        match args.data {
            // Indirection vers `Models::execute` : résout l'expert puis les
            // tools qu'il a le droit d'appeler (voir `Expert::allowed_tools`),
            // et relaie sa réponse sur le canal `expert_answer` (voir
            // `session::spec::CommonSpec::expert`) plutôt que dans le
            // résultat du frame — c'est ce canal, pas `FrameResult`, que lit
            // l'appelant via `FrameSpecRef::ExpertAggregator`.
            FrameKind::ExpertConsultation => {
                let expert_id: ExpertId = channels.read("expert_id")?;
                let task: String = channels.read("expert_consultation_task")?;

                let expert = match self.experts.get(&expert_id).await {
                    Ok(Some(expert)) => expert,
                    Ok(None) => {
                        let error = format!("expert {expert_id} introuvable");
                        return Ok(FrameResponse { frame_id: args.frame_id, updates: Vec::new(), new_logs: Vec::new(), result: FrameResult::Failed(error) });
                    }
                    Err(error) => {
                        return Ok(FrameResponse { frame_id: args.frame_id, updates: Vec::new(), new_logs: Vec::new(), result: FrameResult::Failed(error.to_string()) });
                    }
                };

                let mut tools = Vec::with_capacity(expert.allowed_tools.len());
                for tool_id in &expert.allowed_tools {
                    match self.tools.get(tool_id).await {
                        Ok(Some(tool)) => tools.push(tool),
                        Ok(None) => {
                            let error = format!("tool {tool_id} introuvable pour l'expert {expert_id}");
                            return Ok(FrameResponse { frame_id: args.frame_id, updates: Vec::new(), new_logs: Vec::new(), result: FrameResult::Failed(error) });
                        }
                        Err(error) => {
                            return Ok(FrameResponse { frame_id: args.frame_id, updates: Vec::new(), new_logs: Vec::new(), result: FrameResult::Failed(error.to_string()) });
                        }
                    }
                }

                let context = Context::from(vec![
                    ContextEntry { role: Role::System, content: expert.prompt },
                    ContextEntry { role: Role::User, content: task },
                ]);

                let model_args = ExecuteModelArgs::builder()
                    .model(expert.model_id)
                    .context(context)
                    .tools(tools)
                    .build();

                let stream = match self.models.execute(model_args).await {
                    Ok(stream) => stream,
                    Err(error) => {
                        return Ok(FrameResponse { frame_id: args.frame_id, updates: Vec::new(), new_logs: Vec::new(), result: FrameResult::Failed(error.to_string()) });
                    }
                };
                pin_mut!(stream);

                let mut answer = None;
                let mut failure = None;

                while let Some(event) = stream.next().await {
                    match event {
                        ModelStreamEvent::Completed(response) => answer = {
                            response.text
                        },
                        ModelStreamEvent::Failed(message) => failure = Some(message),
                        ModelStreamEvent::TextDelta(_) => {}
                    }
                }

                if let Some(error) = failure {
                    return Ok(FrameResponse { 
                        frame_id: args.frame_id, 
                        updates: channels.into_updates(), 
                        new_logs: Vec::new(), 
                        result: FrameResult::Failed(error) 
                    });
                }

                channels.write("expert_consultation_answer", answer);


                Ok(FrameResponse { frame_id: args.frame_id, updates: channels.into_updates(), new_logs: Vec::new(), result: FrameResult::Completed })
            },
            // Indirection vers `Tools::execute` (voir aussi `Graphs::execute`
            // pour la node-executor, même principe) — le résultat est relayé
            // sur le canal `tool_result` (voir `session::spec::CommonSpec::tool`),
            // pas dans `FrameResult`, sur le même modèle que `AskExpert`
            // ci-dessus.
            FrameKind::ToolCall { name, parameters, .. } => {
                let tool_id = ToolId::from(name);

                match self.tools.execute(args.session_id, &tool_id, parameters).await {
                    Ok(value) => {
                        let update = ChannelUpdate {
                            name: ChannelName::from("tool_result"),
                            value,
                            contributor: args.frame_id,
                        };

                        Ok(FrameResponse { frame_id: args.frame_id, updates: vec![update], new_logs: Vec::new(), result: FrameResult::Completed })
                    }
                    Err(error) => {
                        Ok(FrameResponse { frame_id: args.frame_id, updates: Vec::new(), new_logs: Vec::new(), result: FrameResult::Failed(error.to_string()) })
                    }
                }
            },
            // Indirection vers `Graphs::execute` : le `NodeContext` est créé
            // ici (portée de la frame en cours), pas dans `Graphs::execute`
            // lui-même, qui ne connaît que la `NodeSpec` — `Graphs` reste
            // ainsi indépendante de la notion de session/frame.
            FrameKind::GraphNode { graph_ref, node_id } => {
                let graph = match self.graphs.get(&graph_ref).await {
                    Ok(graph) => graph,
                    Err(error) => {
                        return Ok(FrameResponse { frame_id: args.frame_id, updates: Vec::new(), new_logs: Vec::new(), result: FrameResult::Failed(error.to_string()) });
                    }
                };

                let Some(spec) = graph.nodes.get(&node_id) else {
                    let error = format!("nœud {node_id:?} introuvable dans le graphe {graph_ref:?}");
                    return Ok(FrameResponse { frame_id: args.frame_id, updates: Vec::new(), new_logs: Vec::new(), result: FrameResult::Failed(error) });
                };

                let ctx = NodeContext::new(args.session_id, args.frame_id, channels, RunLogs::new(args.logs));

                let (updates, new_logs, result) = match self.graphs.execute(spec, ctx).await {
                    Ok((ctx, result)) => (ctx.channels.into_updates(), ctx.logs.into_new_logs(), result),
                    Err(error) => (Vec::new(), Vec::new(), FrameResult::Failed(error.to_string())),
                };

                Ok(FrameResponse { frame_id: args.frame_id, updates, new_logs, result })
            }
            _ => todo!("...")
        }
    }
}