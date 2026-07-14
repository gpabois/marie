use leptos::prelude::*;

use crate::types::{FrameView, SessionLogView, SessionView};

/// Itère réactivement sur les frames de `session` (voir `SessionView::frames`)
/// et délègue tout le rendu à `render_frame` — ce composant n'émet lui-même
/// aucun élément ni classe : il n'existe que pour encapsuler la réactivité
/// (`<For>`, clé stable par `frame.id`) que chaque consommateur devrait sinon
/// réimplémenter à l'identique. La structure DOM, les classes, tout le
/// visuel sont entièrement à la charge de `render_frame` — pattern
/// *headless* : ce crate fournit la donnée et son cycle de vie réactif,
/// jamais son apparence.
///
/// `render_frame` reçoit un [`FrameView`] complet (contexte, stdio/stderr,
/// statut inclus) : formater un statut se fait via
/// [`crate::types::AgentStatusView::label`]/[`AgentStatusView::detail`], un
/// rôle de contexte via [`crate::types::RoleView::label`] — ce crate ne
/// fournit que ces chaînes, jamais de markup ni de classe CSS toute faite.
///
/// `render_frame` doit être `Send + Sync` (voir [`Callback`]) — utilisez
/// [`leptos::callback::UnsyncCallback`] à la place si votre fermeture de
/// rendu capture des types qui ne le sont pas.
#[component]
pub fn SessionFrames(
    #[prop(into)] session: Signal<SessionView>,
    #[prop(into)] render_frame: Callback<FrameView, AnyView>,
) -> impl IntoView {
    view! {
        <For
            each=move || session.get().frames
            key=|frame| frame.id.clone()
            let:frame
        >
            {render_frame.run(frame)}
        </For>
    }
}

/// Itère réactivement sur le journal de `session` (voir `SessionView::logs`)
/// et délègue tout le rendu à `render_log` — même principe que
/// [`SessionFrames`] (voir sa doc), en composant séparé plutôt que combiné
/// pour que chaque consommateur choisisse librement sa disposition (côte à
/// côte, onglets, journal seul, ...) au lieu d'un ordre imposé par ce crate.
#[component]
pub fn SessionLogs(
    #[prop(into)] session: Signal<SessionView>,
    #[prop(into)] render_log: Callback<SessionLogView, AnyView>,
) -> impl IntoView {
    view! {
        <For
            each=move || session.get().logs
            key=|log| log.id.clone()
            let:log
        >
            {render_log.run(log)}
        </For>
    }
}
