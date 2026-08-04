use std::collections::HashMap;

use marie::{
    LocalMarie,
    events::EventSubscription,
    expert::ExpertId,
    hitl::{Answer, Hitl, QuestionKind, model::Answers},
    session::{SessionId, SessionStatus, protocol::SessionEvent},
};
use tokio_stream::StreamExt;

use crate::repl::{Repl, render, state::SessionKind};

pub async fn run(repl: &mut Repl, marie: LocalMarie, kind: SessionKind) {
    match kind {
        SessionKind::Consult { expert_id, task } => run_consult(repl, &marie, expert_id, task).await,
        SessionKind::Live(session_id) => run_live(repl, &marie, session_id).await,
    }
}

/// Consultation directe — contourne entièrement la session (voir
/// `Experts::consult` côté marie-core) : déjà synchrone, donc pas de
/// boucle d'évènements ici, juste un appel puis retour immédiat à l'état
/// précédent (déjà dépilé par `Repl::run` avant cet appel).
async fn run_consult(repl: &Repl, marie: &LocalMarie, expert_id: ExpertId, task: String) {
    repl.input.print_line(format!("consultation de l'expert '{expert_id}'…"));

    match marie.experts.consult(&expert_id, task).await {
        Ok(response) => repl.input.print_line(response.text.unwrap_or_else(|| "(aucune réponse)".to_string())),
        Err(err) => repl.input.print_line(format!("erreur: {err}")),
    }
}

async fn run_live(repl: &Repl, marie: &LocalMarie, session_id: SessionId) {
    let client = marie.session_client_factory.create(session_id);

    let view = match client.get_session().await {
        Ok(view) => view,
        Err(err) => {
            repl.input.print_line(format!("erreur: {err}"));
            return;
        }
    };

    for log in &view.logs {
        repl.input.print_line(render::format_log(log));
    }

    let mut events = client.stream_session_events();

    for pending in &view.pendings {
        let answers = ask_hitl(repl, &mut events, &pending.hitl).await;
        if let Err(err) = client.hitl_reply(pending.id, answers).await {
            repl.input.print_line(format!("erreur lors de la réponse hitl: {err}"));
        }
    }

    if is_terminal(&view.status) {
        repl.input.print_line(format!("session déjà terminée: {:?}", view.status));
        return;
    }

    loop {
        let Some(env) = events.next().await else { break };
        match env.payload {
            SessionEvent::LogCreated(log) | SessionEvent::LogUpdated(log) => repl.input.print_line(render::format_log(&log)),
            SessionEvent::HitlRequested(req) => {
                let answers = ask_hitl(repl, &mut events, &req.hitl).await;
                if let Err(err) = client.hitl_reply(req.id, answers).await {
                    repl.input.print_line(format!("erreur lors de la réponse hitl: {err}"));
                }
            }
            SessionEvent::HitlAnswered(_) => {}
            SessionEvent::SessionStatusUpdated(_, status) => {
                if is_terminal(&status) {
                    repl.input.print_line(format!("session terminée: {status:?}"));
                    break;
                }
            }
        }
    }
}

fn is_terminal(status: &SessionStatus) -> bool {
    // `SessionStatus` n'a pas de variante `Completed` à ce jour (voir le
    // plan) — `Archived` est traitée comme l'équivalent "terminée avec
    // succès" du `completed` de la spécification.
    matches!(status, SessionStatus::Archived | SessionStatus::Failed(_))
}

async fn ask_hitl(repl: &Repl, events: &mut EventSubscription<SessionEvent>, hitl: &Hitl) -> Answers {
    match hitl {
        Hitl::Text => {
            let text = read_with_live_events(repl, events, "réponse> ").await;
            Answers::from(HashMap::from([("text".to_string(), Answer::Single(text))]))
        }
        Hitl::Questions(questions) => {
            let mut map = HashMap::new();
            for q in questions {
                repl.input.print_line(render::format_question(q));

                let answer = match &q.kind {
                    QuestionKind::LongText => {
                        let mut lines = Vec::new();
                        loop {
                            let line = read_with_live_events(repl, events, "> ").await;
                            if line == "." {
                                break;
                            }
                            lines.push(line);
                        }
                        Answer::Single(lines.join("\n"))
                    }
                    QuestionKind::Checkboxes { options } => {
                        let raw = read_with_live_events(repl, events, "> ").await;
                        let items = raw
                            .split(',')
                            .map(|s| s.trim())
                            .filter(|s| !s.is_empty())
                            .map(|s| resolve_option(options, s))
                            .collect();
                        Answer::Multiple(items)
                    }
                    QuestionKind::Select { options } | QuestionKind::Radio { options } => {
                        let raw = read_with_live_events(repl, events, "> ").await;
                        Answer::Single(resolve_option(options, raw.trim()))
                    }
                    QuestionKind::ShortText | QuestionKind::FileUpload { .. } => {
                        Answer::Single(read_with_live_events(repl, events, "> ").await)
                    }
                };

                map.insert(q.key.clone(), answer);
            }
            Answers::from(map)
        }
    }
}

fn resolve_option(options: &[String], raw: &str) -> String {
    if let Ok(idx) = raw.parse::<usize>() {
        if idx >= 1 && idx <= options.len() {
            return options[idx - 1].clone();
        }
    }
    raw.to_string()
}

/// Attend une ligne de l'utilisateur tout en continuant à imprimer les
/// évènements (logs) qui arrivent sur `events` entre-temps — la future de
/// lecture est créée une seule fois puis épinglée (`tokio::pin!`) pour être
/// re-pollée à chaque tour de la boucle `select!` sans redemander une
/// nouvelle ligne à chaque itération (voir `InputHandle::read_line`, qui
/// enverrait sinon une nouvelle commande `ReadLine` à chaque repassage).
async fn read_with_live_events(repl: &Repl, events: &mut EventSubscription<SessionEvent>, prompt: &str) -> String {
    let read_fut = repl.input.read_line(prompt);
    tokio::pin!(read_fut);

    loop {
        tokio::select! {
            line = &mut read_fut => return line.unwrap_or_default(),
            Some(env) = events.next() => {
                if let SessionEvent::LogCreated(log) | SessionEvent::LogUpdated(log) = env.payload {
                    repl.input.print_line(render::format_log(&log));
                }
            }
        }
    }
}
