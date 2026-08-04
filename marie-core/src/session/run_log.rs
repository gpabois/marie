use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{expert::RequestAskExpert, graph::GraphId, hitl::Hitl, tools::RequestToolCall};

/// Ce qu'un point de yield a demandé — porte la requête d'origine (pour
/// l'audit de déterminisme du rejeu, voir la doc de [`RunLogs`]), jamais la
/// réponse : celle-ci vit dans [`RunLog::value`], remplie plus tard par
/// `SessionHandler::resolve_pending_log`. Les requêtes elles-mêmes
/// (`RequestAskExpert`/`RequestToolCall`/`Hitl`) ne portent volontairement
/// aucun id — voir leur doc — l'id du frame enfant créé pour chacune est
/// généré côté `SessionHandler`, jamais ici.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RunLogContent {
    /// Un seul appel `ask_experts!` peut demander plusieurs experts en une
    /// fois (fan-out sous un même `ExpertAggregator`) — un seul `RunLog` par
    /// appel, pas un par requête.
    ConsultExpertsLog { requests: Vec<RequestAskExpert> },
    CallToolsLog { requests: Vec<RequestToolCall> },
    /// `graph_id` : identifiant demandé par `FrameResult::ExecuteGraph`,
    /// avant résolution en `GraphRef` par `SessionHandler::append_graph`.
    GraphLog { graph_id: GraphId },
    HitlLog { request: Hitl },
}

/// Une réservation, indexée par ordre d'appel (voir [`RunLogs::reserve_log`])
/// — `value: None` tant qu'aucun enfant pertinent n'a terminé, `Some(_)`
/// une fois résolue par `SessionHandler::resolve_pending_log`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunLog {
    pub index: u32,
    pub content: RunLogContent,
    pub value: Option<Value>,
}

/// Poignée opaque renvoyée par [`RunLogs::reserve_log`] — un simple index
/// dans `RunLogs::entries`, pas une clé fournie par l'appelant : l'identité
/// d'une réservation est entièrement déterminée par sa position d'appel (voir
/// la doc de [`RunLogs`]).
#[derive(Debug, Clone, Copy)]
pub struct RunLogRef(pub(crate) usize);

/// Compteur déterministe rejouable : à chaque exécution de
/// `Nodable::execute`, [`Self::reserve_log`] avance un curseur qui repart de
/// zéro (voir [`Self::new`]) et retrouve/complète séquentiellement les mêmes
/// entrées de `FrameNode.logs`, tant que le corps du node appelle
/// `reserve_log`/`check` toujours dans le même ordre avant tout point de
/// suspension.
///
/// Invariant de sûreté du rejeu : tout `reserve_log` précédent, dans la même
/// exécution, est nécessairement déjà résolu — sinon le corps du node aurait
/// déjà retourné (idiome « check puis yield »). Un rejeu qui atteint le
/// point d'appel `k` a donc forcément emprunté les mêmes branches que la
/// première exécution pour y arriver : au plus une entrée non résolue peut
/// exister sur un frame à un instant donné. Corollaire : ne jamais brancher
/// sur une source de non-déterminisme ambiante (aléa, horloge, variable
/// d'environnement) avant un `reserve_log`/`check` — seulement sur des
/// valeurs lues via `ctx.read`/`ctx.logs.check` ou des arguments, sous peine
/// de désynchroniser le compteur au rejeu.
#[derive(Debug, Clone)]
pub struct RunLogs {
    entries: Vec<RunLog>,
    persisted_len: usize,
    cursor: usize,
}

impl RunLogs {
    /// `persisted` : `FrameNode.logs` tel que chargé avant ce run (voir
    /// `RunFrameArgs::logs`, lui-même peuplé depuis `FrameTree::logs_of` dans
    /// `SessionHandler::run_frame`).
    pub fn new(persisted: Vec<RunLog>) -> Self {
        let persisted_len = persisted.len();
        Self { entries: persisted, persisted_len, cursor: 0 }
    }

    /// Crée l'entrée à la position courante si elle n'existe pas encore ;
    /// sinon renvoie la poignée de l'entrée déjà persistée sans y toucher
    /// (son `content` d'origine fait foi, pas celui recalculé par ce run).
    pub fn reserve_log(&mut self, content: RunLogContent) -> RunLogRef {
        let index = self.cursor;
        self.cursor += 1;
        if index >= self.entries.len() {
            self.entries.push(RunLog { index: index as u32, content, value: None });
        }
        RunLogRef(index)
    }

    /// `Some(value)` désérialisable par l'appelant si `log_ref` est déjà
    /// résolue, `None` sinon.
    pub fn check(&self, log_ref: &RunLogRef) -> Option<Value> {
        self.entries[log_ref.0].value.clone()
    }

    /// Entrées créées par CE run (au-delà de `persisted_len`) — devient
    /// `FrameResponse::new_logs`. Par construction (voir la doc de
    /// [`Self`]), au plus une seule entrée nouvellement créée peut rester non
    /// résolue à la fin d'un run.
    pub fn into_new_logs(self) -> Vec<RunLog> {
        self.entries.into_iter().skip(self.persisted_len).collect()
    }
}
