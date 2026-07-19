use async_trait::async_trait;
use serde_json::Value;

use crate::{
    agent::status::{AgentStatus, YieldStatus},
    job::Job,
    network::worker::{JobContext, client::WorkerClient},
    rpc::Void,
    session::{
        client::SessionClient,
        state::{
            executable::{GraphRuntime, NodeOutcome, RustRegistry},
            frame::{GraphFrame, GraphResponse, GraphStackFrame},
            hitl::HitlFrameId,
            orchestration::{OrchestrationFrameId, Waiter},
        },
        worker::RunAgent,
    },
};

/// Job qui pilote un [`GraphFrame`] pas à pas — même discipline "un pas par
/// Job" que [`RunAgent`] ("un tour par Job") : ce `Job` exécute et fait
/// avancer *un seul* curseur (voir [`crate::session::state::Cursor`]) avant
/// de se terminer, persistant systématiquement la progression avant de
/// resoumettre un nouveau Job (soi-même pour continuer, `RunAgent` pour un
/// enfant spawné par un nœud `Agent`) ou de s'arrêter (curseur yieldé,
/// racine conclue).
pub struct RunGraphStep {
    sessions: SessionClient,
    worker: WorkerClient,
    registry: RustRegistry,
    runtime: GraphRuntime,
}

impl RunGraphStep {
    #[must_use]
    pub fn new(sessions: SessionClient, worker: WorkerClient, registry: RustRegistry, runtime: GraphRuntime) -> Self {
        Self { sessions, worker, registry, runtime }
    }
}

/// `true` si tous les curseurs du sommet de la pile de `frame` ont conclu
/// (`AgentStatus::Finished`) — `false` si `cursors` est vide (curseurs
/// parqués en attente d'un rendez-vous, voir [`crate::session::state::AdvanceOutcome::ParkedAtJoin`]),
/// pour ne pas conclure prématurément sur une liste temporairement vide.
fn stack_top_finished(frame: &GraphFrame) -> bool {
    let cursors = &frame.top().graph.cursors;
    !cursors.is_empty() && cursors.iter().all(|cursor| cursor.status == AgentStatus::Finished)
}

/// Dépile le niveau conclu (voir [`stack_top_finished`]) et reprend le
/// curseur du niveau parent qui l'avait poussé (voir [`crate::session::state::executable::Executable::Subgraph`]) :
/// injecte la sortie du sous-graphe comme `last_output` de ce curseur, puis
/// le fait *avancer* (pas ré-exécuter — l'action du nœud `Subgraph` a déjà
/// eu lieu, c'est tout le sous-graphe qu'elle représentait) au-delà du nœud
/// `Subgraph` qui l'avait poussé.
async fn pop_and_resume(frame: &mut GraphFrame, registry: &RustRegistry) -> anyhow::Result<()> {
    let finished = frame.stack.pop().expect("stack_top_finished garantit une pile non vide");
    let output = finished.graph.cursors.first().map(|cursor| cursor.last_output.clone()).unwrap_or(Value::Null);

    let Some(return_node) = finished.return_node else {
        return Ok(());
    };

    let Some(cursor_id) = frame.top().graph.cursors.iter().find(|cursor| cursor.current == return_node).map(|cursor| cursor.id) else {
        return Ok(());
    };

    if let Some(cursor) = frame.top_mut().graph.cursors.iter_mut().find(|cursor| cursor.id == cursor_id) {
        cursor.last_output = output;
    }

    frame.top_mut().graph.advance_cursor(cursor_id, registry).await?;
    Ok(())
}

#[async_trait]
impl Job for RunGraphStep {
    const NAME: &'static str = "marie/sessions/run-graph-step";

    type Args = GraphFrame;
    type Return = Void;

    async fn execute(self, mut frame: GraphFrame, _cx: JobContext) -> Result<Self::Return, anyhow::Error> {
        let session_id = frame.id.session_id();

        if stack_top_finished(&frame) {
            if frame.stack.len() == 1 {
                let output = frame.top().graph.cursors.first().map(|cursor| cursor.last_output.clone()).unwrap_or(Value::Null);
                self.sessions.report_graph_run(frame.id, GraphResponse::Finished { output }).await?;
            } else {
                pop_and_resume(&mut frame, &self.registry).await?;
                self.sessions.update_graph_step(frame.id, frame.clone()).await?;
                self.worker.spawn::<RunGraphStep>(frame, None).await?;
            }
            return Ok(Void);
        }

        let Some(cursor_id) = frame.top().graph.ready_cursor().map(|cursor| cursor.id) else {
            // Tous les curseurs actifs sont `Yielding` (en attente d'un
            // évènement externe) ou parqués dans `graph.joins` (en attente
            // d'un rendez-vous pas encore complet) : rien à faire dans
            // l'immédiat, ce `GraphFrame` sera repris par le prochain
            // rapport (`report_agent_run`/`report_graph_run`) ou, pour un
            // rendez-vous, par le prochain pas qui débloquera le curseur
            // frère manquant.
            self.sessions.update_graph_step(frame.id, frame).await?;
            return Ok(Void);
        };

        let outcome = frame.top_mut().graph.execute_cursor(cursor_id, &self.registry, Some(&self.runtime), session_id).await;

        let node_outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                let message = error.to_string();
                frame.error = message.clone();
                self.sessions.report_graph_run(frame.id, GraphResponse::Failed { error: message }).await?;
                return Ok(Void);
            }
        };

        match node_outcome {
            Some(NodeOutcome::Yield(_)) => {
                self.sessions.update_graph_step(frame.id, frame).await?;
                return Ok(Void);
            }
            Some(NodeOutcome::SpawnAgent(child)) => {
                self.sessions.report_graph_dispatch(frame.id, frame, child.clone()).await?;
                self.worker.spawn::<RunAgent>(child, None).await?;
                return Ok(Void);
            }
            Some(NodeOutcome::SpawnOrchestration { strategy, children }) => {
                let orchestration_id = OrchestrationFrameId::new(session_id, crate::id::generate_id());

                if let Some(cursor) = frame.top_mut().graph.cursors.iter_mut().find(|cursor| cursor.id == cursor_id) {
                    cursor.status = AgentStatus::Yielding(YieldStatus::WaitingOrchestration { orchestration: orchestration_id });
                }

                self.sessions.push_orchestration(session_id, orchestration_id, Waiter::Graph(frame.id), Some(frame), strategy, children).await?;
                return Ok(Void);
            }
            Some(NodeOutcome::EnterSubgraph(subgraph)) => {
                let return_node = frame.top().graph.cursor(cursor_id).map(|cursor| cursor.current.clone());
                frame.stack.push(GraphStackFrame { graph: subgraph, return_node });

                self.sessions.update_graph_step(frame.id, frame.clone()).await?;
                self.worker.spawn::<RunGraphStep>(frame, None).await?;
                return Ok(Void);
            }
            Some(NodeOutcome::AskUserInput { questions }) => {
                let hitl_id = HitlFrameId::new(session_id, crate::id::generate_id());

                if let Some(cursor) = frame.top_mut().graph.cursors.iter_mut().find(|cursor| cursor.id == cursor_id) {
                    cursor.status = AgentStatus::Yielding(YieldStatus::WaitingHitl { hitl: hitl_id });
                }

                self.sessions.push_hitl(hitl_id, Waiter::Graph(frame.id), questions, Some(frame)).await?;
                return Ok(Void);
            }
            Some(NodeOutcome::Value(_)) | None => {}
        }

        if let Err(error) = frame.top_mut().graph.advance_cursor(cursor_id, &self.registry).await {
            let message = error.to_string();
            frame.error = message.clone();
            self.sessions.report_graph_run(frame.id, GraphResponse::Failed { error: message }).await?;
            return Ok(Void);
        }

        self.sessions.update_graph_step(frame.id, frame.clone()).await?;
        self.worker.spawn::<RunGraphStep>(frame, None).await?;
        Ok(Void)
    }
}
