//! Quatre macros ergonomiques pour les corps de node natives (`core_node!`/
//! `node!`, voir [`crate::graph::node::Nodable::execute`]) qui doivent
//! demander l'avis d'experts, l'exécution d'outils, d'un sous-graphe, ou une
//! réponse HITL — chacune enveloppe le même idiome réserve/vérifie sur
//! [`crate::graph::node::NodeContext::logs`] (voir
//! [`crate::session::run_log::RunLogs`]) : si la réservation est déjà
//! résolue, la valeur typée est renvoyée directement (comme un appel
//! bloquant qui vient de terminer) ; sinon le corps du node retourne
//! immédiatement le [`crate::session::protocol::FrameResult`] correspondant,
//! laissant `SessionHandler` créer le(s) frame(s) enfant(s) qui la
//! résoudront plus tard.
//!
//! Simples `macro_rules!` plutôt que des proc-macros dans `marie-macros` :
//! contrairement à `node!`/`spec!`, elles ne font qu'assembler un
//! `RunLogContent`, appeler `reserve_log`/`check`, et retourner tôt sur
//! `None` — aucune analyse structurelle n'est nécessaire.

/// `let (foo, bar): (Foo, Bar) = ask_experts!(ctx, ask_a, ask_b);` — une
/// seule réservation par appel, quel que soit le nombre de requêtes
/// demandées en une fois (fan-out sous un même `ExpertAggregator`, voir
/// `SessionHandler::append_experts_askings`), résolue comme un unique
/// tableau JSON (voir `SessionHandler::resolve_pending_log`).
#[macro_export]
macro_rules! ask_experts {
    ($ctx:expr, $($request:expr),+ $(,)?) => {{
        let __requests = vec![$($request),+];
        let __log = $ctx.logs.reserve_log(
            $crate::session::run_log::RunLogContent::AskExpertLog { requests: __requests.clone() }
        );
        match $ctx.logs.check(&__log) {
            ::std::option::Option::Some(__value) => {
                let __values: ::std::vec::Vec<::serde_json::Value> = ::serde_json::from_value(__value)
                    .expect("RunLogs: AskExpertLog résolu n'est pas un tableau");
                let mut __iter = __values.into_iter();
                // Virgule finale ajoutée systématiquement : sans elle, une
                // seule requête produirait `( expr )` — une simple
                // parenthésation, pas un 1-tuple (`(expr,)`). `$request`
                // n'est référencé qu'à travers `stringify!` (purement
                // syntaxique, sans évaluation) : il a déjà été consommé une
                // fois dans `__requests` ci-dessus, le réévaluer ici
                // déplacerait ses valeurs une seconde fois.
                ( $( {
                    let _ = stringify!($request);
                    ::serde_json::from_value(__iter.next().expect("RunLogs: arité résolue insuffisante pour AskExpertLog"))
                        .expect("RunLogs: réponse d'expert non désérialisable")
                } ),+ ,)
            }
            ::std::option::Option::None => {
                return Ok($crate::session::protocol::FrameResult::AskExperts(__requests));
            }
        }
    }};
}

/// `let (foo, bar): (Foo, Bar) = call_tools!(ctx, tc_a, tc_b);` — même
/// principe que [`ask_experts!`], pour `FrameResult::RequestToolsCalls`/
/// `ToolAggregator`.
#[macro_export]
macro_rules! call_tools {
    ($ctx:expr, $($request:expr),+ $(,)?) => {{
        let __requests = vec![$($request),+];
        let __log = $ctx.logs.reserve_log(
            $crate::session::run_log::RunLogContent::ToolCallLog { requests: __requests.clone() }
        );
        match $ctx.logs.check(&__log) {
            ::std::option::Option::Some(__value) => {
                let __values: ::std::vec::Vec<::serde_json::Value> = ::serde_json::from_value(__value)
                    .expect("RunLogs: ToolCallLog résolu n'est pas un tableau");
                let mut __iter = __values.into_iter();
                // Voir les mêmes remarques dans `ask_experts!` (virgule
                // finale systématique, `stringify!` pour ne pas réévaluer
                // `$request` une seconde fois).
                ( $( {
                    let _ = stringify!($request);
                    ::serde_json::from_value(__iter.next().expect("RunLogs: arité résolue insuffisante pour ToolCallLog"))
                        .expect("RunLogs: résultat d'outil non désérialisable")
                } ),+ ,)
            }
            ::std::option::Option::None => {
                return Ok($crate::session::protocol::FrameResult::RequestToolsCalls(__requests));
            }
        }
    }};
}

/// `let answer: MyAnswer = hitl!(ctx, Hitl::Question(q));`
#[macro_export]
macro_rules! hitl {
    ($ctx:expr, $request:expr) => {{
        let __request = $request;
        let __log = $ctx.logs.reserve_log(
            $crate::session::run_log::RunLogContent::HitlLog { request: __request.clone() }
        );
        match $ctx.logs.check(&__log) {
            ::std::option::Option::Some(__value) => ::serde_json::from_value(__value)
                .expect("RunLogs: réponse HITL non désérialisable"),
            ::std::option::Option::None => {
                return Ok($crate::session::protocol::FrameResult::RequestHitl(__request));
            }
        }
    }};
}

/// `let outputs: HashMap<ChannelName, Value> = graph!(ctx, graph_id);`
#[macro_export]
macro_rules! graph {
    ($ctx:expr, $graph_id:expr) => {{
        let __graph_id = $graph_id;
        let __log = $ctx.logs.reserve_log(
            $crate::session::run_log::RunLogContent::GraphLog { graph_id: __graph_id.clone() }
        );
        match $ctx.logs.check(&__log) {
            ::std::option::Option::Some(__value) => {
                let __map: ::std::collections::HashMap<$crate::session::channel::ChannelName, ::serde_json::Value> =
                    ::serde_json::from_value(__value).expect("RunLogs: sortie de sous-graphe non désérialisable");
                __map
            }
            ::std::option::Option::None => {
                return Ok($crate::session::protocol::FrameResult::ExecuteGraph(__graph_id));
            }
        }
    }};
}
