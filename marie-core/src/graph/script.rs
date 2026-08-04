use std::sync::Arc;

use moka::future::Cache;
use rune::support::Result;
use rune::termcolor::{ColorChoice, StandardStream};
use rune::{Context, Diagnostics, Module, Options, Source, Sources, Unit, Vm};

use crate::expert::RequestAskExpert;
use crate::graph::GraphId;
use crate::graph::node::{NodeContext, NodeId};
use crate::hitl::Hitl;
use crate::session::protocol::{Branch, FrameResult};
use crate::session::run_log::RunLogContent;
use crate::tools::RequestToolCall;

/// Moteur d'exécution des nœuds/arêtes de type script (Rune) d'un
/// [`crate::graph::GraphSpec`] — alternative à [`crate::graph::node::Nodable`]
/// (qui exige de recompiler le binaire hôte) pour de la logique qu'un
/// opérateur du cluster veut pouvoir écrire/modifier sans redéploiement :
/// typiquement un **routeur conditionnel** entre plusieurs nœuds suivants
/// (voir l'exemple plus bas), mais aussi n'importe quelle logique de nœud
/// simple (lire des canaux, en écrire d'autres, décider de la suite).
///
/// # Contrat d'un script
///
/// Un script doit déclarer une fonction `pub fn main(ctx) { ... }` (arité
/// fixe : [`Self::execute`] appelle toujours `main` avec exactement cet
/// argument, une fonction qui en déclare un nombre différent échoue à
/// l'exécution) :
///
/// - `ctx` est le pont vers le [`crate::graph::node::NodeContext`] natif de
///   ce pas d'exécution (voir `RuneNodeContext`, installé dans le
///   [`Context`] Rune par [`GraphScriptEngine::new`]) — toute donnée dont le
///   script a besoin transite par un canal hérité lu via `ctx.read`, il n'y
///   a pas de charge utile séparée.
///
/// - `ctx.read(name)` relit un canal (converti automatiquement en valeur
///   Rune, voir [`json_to_value`]) ; échoue si `name` n'existe pas dans les
///   canaux hérités **ni** dans les [`crate::session::channel::ChannelUpdate`]
///   déjà écrits plus tôt dans le même pas.
/// - `ctx.write(name, value)` n'écrase jamais directement l'état : elle
///   empile une nouvelle contribution (voir la doc de `NodeContext`),
///   relisible immédiatement par un `ctx.read(name)` suivant.
///
/// `main` doit se terminer par l'une des commandes *terminales* ci-dessous —
/// chacune renvoie un [`rune::Value`] enveloppé dans un `Result` Rune (parce
/// qu'elle peut elle-même échouer à convertir ses arguments), à dérouler
/// avec `?` comme n'importe quel appel faillible :
///
/// | Commande | Équivalent [`FrameResult`] |
/// |---|---|
/// | `next()` | [`FrameResult::Continue`] — passe au nœud suivant de l'arête par défaut |
/// | `goto(node_id)` | [`FrameResult::GoTo`] — saute explicitement à `node_id` |
/// | `fork(branches, join)` | [`FrameResult::Fork`] — plusieurs branches en parallèle, rejointes à `join` |
/// | `completed()` | [`FrameResult::Completed`] |
/// | `failed(message)` | [`FrameResult::Failed`] |
///
/// Quatre commandes supplémentaires — `hitl(request)`, `ask_experts(asks)`,
/// `call_tools(calls)`, `graph(graph_id)` — ne renvoient **pas** directement
/// un [`FrameResult`] : elles construisent une simple description de la
/// demande, à suspendre explicitement via le mot-clé natif `yield` de Rune
/// (voir [Generator](https://rune-rs.github.io/book/generators.html)) :
///
/// ```text
/// let answer = yield hitl(request)?;
/// let answers = yield ask_experts(asks)?;
/// let results = yield call_tools(calls)?;
/// let outputs = yield graph(graph_id)?;
/// ```
///
/// [`Self::execute`] pilote lui-même le générateur ainsi produit par `main`
/// (dès qu'il contient un `yield`, Rune en fait une fonction génératrice —
/// voir [`rune::runtime::Generator`]) : à chaque suspension, il consulte le
/// journal de rejeu du frame en cours ([`crate::session::run_log::RunLogs`],
/// exposé par [`NodeContext::logs`]) — si la demande est déjà résolue (un
/// frame enfant correspondant a déjà terminé lors d'un rejeu précédent), la
/// réponse est aussitôt réinjectée dans le générateur (`resume_with`), qui
/// reprend son exécution *sans jamais rendre la main à l'appelant* ; sinon
/// l'exécution s'arrête là et la demande est traduite en [`FrameResult`]
/// (`RequestHitl`/`AskExperts`/`RequestToolsCalls`/`ExecuteGraph`) exactement
/// comme les macros Rust `hitl!`/`ask_experts!`/`call_tools!`/`graph!` (voir
/// [`crate::graph::run_log_macros`]) — même mécanisme de
/// rejeu déterministe, juste piloté depuis Rust plutôt que depuis le corps
/// du node. Un script qui n'utilise jamais `yield` s'exécute exactement comme
/// avant (le premier `resume()` va jusqu'au bout et renvoie directement la
/// valeur finale).
///
/// La valeur renvoyée par `main` (à la complétion du générateur, ou par un
/// appel synchrone si aucun `yield` n'a jamais été atteint) est reconvertie
/// côté Rust en [`FrameResult`] par [`GraphScriptEngine::execute`] (même
/// passerelle JSON, voir [`value_to_type`]) — appeler une commande sans la
/// dérouler avec `?` (ex: `goto("x")` au lieu de `goto("x")?`) laisserait
/// `main` renvoyer un `Result` Rune brut, que cette conversion rejetterait.
///
/// # Exemple — nœud simple
///
/// ```text
/// pub fn main(ctx) {
///     let name = ctx.read("name")?;
///     ctx.write("greeting", `Bonjour, ${name} !`)?;
///     completed()?
/// }
/// ```
///
/// # Exemple — routeur conditionnel entre nœuds
///
/// Le cas d'usage principal des scripts : [`crate::graph::GraphSpec::edges`]
/// n'est qu'une correspondance fixe `NodeId -> NodeId`, sans notion de garde
/// conditionnelle — un nœud `Rune` est le seul moyen de décider du nœud
/// suivant à l'exécution, selon l'état courant du graphe (ici : relecture
/// d'une revue de code, `max_attempts` porté par un canal hérité plutôt
/// qu'une constante figée dans le script).
///
/// ```text
/// pub fn main(ctx) {
///     let status = ctx.read("review_status")?;
///     let attempts = ctx.read("review_attempts")?;
///     let max_attempts = ctx.read("max_attempts")?;
///
///     match status {
///         "approved" => goto("merge")?,
///         "changes_requested" if attempts < max_attempts => goto("revise")?,
///         "changes_requested" => failed(`revue non résolue après ${attempts} tentatives`)?,
///         "rejected" => failed(`revue rejetée : ${status}`)?,
///         _ => next()?,
///     }
/// }
/// ```
///
/// # Exemples — suspendre via `yield`
///
/// Les quatre commandes `ask_experts`/`call_tools`/`hitl`/`graph` prennent
/// et renvoient les mêmes formes que leurs équivalents [`FrameResult`] (voir
/// le tableau plus haut) — `ask_experts`/`call_tools` acceptent un tableau
/// et renvoient un tableau de réponses dans le même ordre (une seule
/// suspension pour tout le lot, pas une par requête), `hitl` une seule
/// [`Hitl`] et renvoie la réponse brute, `graph` un id de graphe (chaîne) et
/// renvoie les canaux exportés par ce sous-graphe sous forme d'objet.
///
/// Ni `ask_experts`/`call_tools` ni le script qui les appelle n'ont à fournir
/// d'identifiant de corrélation : `SessionHandler` en génère un pour chaque
/// frame enfant créé (voir [`crate::expert::RequestAskExpert`]/
/// [`RequestToolCall`]) — un script s'exécutant potentiellement plusieurs
/// fois (rejeu), lui en faire générer un lui-même produirait une valeur
/// différente à chaque fois. La corrélation des réponses se fait par
/// position dans le tableau, pas par un identifiant.
///
/// **Un ou plusieurs experts** (une réponse par requête, dans l'ordre) :
///
/// ```text
/// pub fn main(ctx) {
///     let task = ctx.read("task")?;
///     let expert_id = ctx.read("expert_id")?;
///
///     let answers = yield ask_experts([
///         #{ task: task, expert_id: expert_id },
///     ])?;
///     ctx.write("answer", answers[0])?;
///     completed()?
/// }
/// ```
///
/// **Un ou plusieurs appels d'outil**, même principe :
///
/// ```text
/// pub fn main(ctx) {
///     let parameters = ctx.read("parameters")?;
///
///     let results = yield call_tools([
///         #{ name: "run-tests", parameters: parameters },
///     ])?;
///     ctx.write("result", results[0])?;
///     completed()?
/// }
/// ```
///
/// **Une validation humaine (HITL)** :
///
/// ```text
/// pub fn main(ctx) {
///     let answer = yield hitl("Text")?;
///     ctx.write("answer", answer)?;
///     completed()?
/// }
/// ```
///
/// **Un sous-graphe**, dont les canaux exportés reviennent sous forme d'objet :
///
/// ```text
/// pub fn main(ctx) {
///     let outputs = yield graph("post-merge-checks")?;
///     ctx.write("post_merge_status", outputs["status"])?;
///     completed()?
/// }
/// ```
#[derive(Clone)]
pub struct GraphScriptEngine {
    context: Arc<Context>,
    /// Cache des `Unit` déjà compilées, indexé par le source du script —
    /// un `Node`/`Edge` `Executable::Rune` porte le même script à chaque
    /// exécution (voir `graph::builtin::execute_script`), recompiler à
    /// chaque appel serait un travail redondant sur le chemin chaud
    /// d'exécution d'un graphe.
    units: Cache<String, Arc<Unit>>
}

impl GraphScriptEngine {
    /// Construit le moteur : un unique [`Context`] Rune (modules par défaut
    /// + [`RuneNodeContext`] et les commandes de routage, voir la doc de
    /// [`Self`]), partagé par tous les scripts compilés/exécutés par cette
    /// instance — coûteux à construire (chargement des modules), pas à
    /// cloner (`Arc`), d'où une seule instance par process plutôt qu'une par
    /// script.
    pub fn new() -> Result<Self> {
        let mut context = Context::with_default_modules()?;

        let mut module = Module::new();
        module.ty::<RuneNodeContext>()?;
        module.function_meta(RuneNodeContext::read)?;
        module.function_meta(RuneNodeContext::write)?;
        module.function_meta(next)?;
        module.function_meta(goto)?;
        module.function_meta(fork)?;
        module.function_meta(hitl)?;
        module.function_meta(ask_experts)?;
        module.function_meta(call_tools)?;
        module.function_meta(graph)?;
        module.function_meta(completed)?;
        module.function_meta(failed)?;
        context.install(module)?;

        Ok(Self { context: Arc::new(context), units: Cache::builder().build() })
    }

    async fn compile(&self, script: &str) -> Result<Arc<Unit>> {
        self.units
            .try_get_with(script.to_string(), self.compile_uncached(script))
            .await
            .map_err(|error| rune::support::Error::msg(error.to_string()))
    }

    async fn compile_uncached(&self, script: &str) -> Result<Arc<Unit>> {
        let mut sources = Sources::new();
        sources.insert(Source::memory(script)?)?;

        let mut diagnostics = Diagnostics::new();
        let options = Options::from_default_env()?;

        // Pas de `options.script(true)` : ce mode compile la source entière
        // comme un unique corps de fonction anonyme (appelée via
        // `Hash::EMPTY`), incompatible avec la déclaration `pub fn main(ctx)`
        // attendue par [`Self::execute`] (`vm.execute(["main"], ...)`, qui
        // cherche une fonction *nommée*) — voir la doc de [`Self`].
        let result = rune::prepare(&mut sources)
            .with_options(&options)
            .with_context(&self.context)
            .with_diagnostics(&mut diagnostics)
            .build();

        if !diagnostics.is_empty() {
            let mut writer = StandardStream::stderr(ColorChoice::Always);
            diagnostics.emit(&mut writer, &sources)?;
        }

        let unit = result?;
        Ok(Arc::new(unit))
    }

    /// Compile `script` (voir [`Self::compile`], mis en cache) puis pilote sa
    /// fonction `main(ctx)` avec un [`RuneNodeContext`] enveloppant `ctx`
    /// (voir la doc de [`Self`] pour le contrat complet, notamment le rôle
    /// des `yield`) jusqu'à sa complétion **ou** jusqu'à une suspension pas
    /// encore résolue dans le journal de rejeu de `ctx` — la valeur qui
    /// termine `main` (ou celle traduite depuis la demande en suspens) est
    /// décodée en [`FrameResult`] via [`value_to_type`]/
    /// [`run_log_content_to_frame_result`].
    pub async fn execute(&self, script: &str, ctx: &mut NodeContext) -> crate::Result<FrameResult> {
        let unit = self.compile(script).await.map_err(|error| crate::err!("{error}"))?;
        let runtime = Arc::new(self.context.runtime().map_err(|error| crate::err!("{error}"))?);
        let mut vm = Vm::new(runtime, unit);

        let mut execution = vm.execute(["main"], (RuneNodeContext::new(ctx),))
            .map_err(|error| crate::err!("{error}"))?;

        // Premier `resume()` sans valeur (voir la doc de
        // `rune::runtime::VmExecution::resume`) : que `main` contienne ou
        // non un `yield`, ceci l'exécute jusqu'à sa première suspension —
        // ou, en l'absence de tout `yield`, jusqu'au bout d'un coup (comme
        // avant cette réécriture, voir la doc de [`Self`]).
        let mut state = execution.resume().into_result().map_err(|error| crate::err!("{error}"))?;

        loop {
            let yielded = match state {
                rune::runtime::GeneratorState::Complete(value) => {
                    return value_to_type(value).map_err(|error| crate::err!("{error}"));
                }
                rune::runtime::GeneratorState::Yielded(value) => value,
            };

            // La valeur suspendue par `yield` est toujours construite par
            // `hitl`/`ask_experts`/`call_tools`/`graph` (voir leur doc) :
            // décodable directement en `RunLogContent`.
            let content: RunLogContent = value_to_type(yielded).map_err(|error| crate::err!("{error}"))?;
            let log_ref = ctx.logs.reserve_log(content.clone());

            let Some(resolved) = ctx.logs.check(&log_ref) else {
                // Encore en attente : on s'arrête là, sans faire progresser
                // le générateur plus loin — un rejeu ultérieur (une fois
                // l'enfant correspondant terminé) recompilera/relancera ce
                // même script depuis le début et retrouvera cette même
                // entrée déjà réservée à la même position d'appel (voir la
                // doc de `crate::session::run_log::RunLogs`).
                return Ok(run_log_content_to_frame_result(content));
            };

            // Déjà résolue lors d'un rejeu précédent : on réinjecte la
            // réponse dans le générateur et on continue, sans jamais rendre
            // la main à l'appelant pour ce `yield`-là.
            let resume_value = json_to_value(resolved).map_err(|error| crate::err!("{error}"))?;
            state = execution.resume_with(resume_value).into_result().map_err(|error| crate::err!("{error}"))?;
        }
    }
}

#[derive(rune::Any, Debug)]
#[rune(item = ::marie)] 
struct RuneNodeContext {
    ctx: *mut NodeContext
}

impl RuneNodeContext {
    pub fn new(ctx: &mut NodeContext) -> Self {
        Self {
            ctx: std::ptr::from_mut(ctx)
        }
    }
}

impl RuneNodeContext {
    pub fn deref_mut(&mut self) -> &mut NodeContext {
        unsafe { self.ctx.as_mut_unchecked() }
    }

    pub fn deref(&self) -> &NodeContext {
        unsafe { self.ctx.as_ref_unchecked() }
    }

    #[rune::function]
    pub fn read(&self, channel_name: String) -> Result<rune::Value, String> {
        let value: serde_json::Value = self.deref().read(channel_name).map_err(|error| error.to_string())?;
        json_to_value(value).map_err(|error| error.to_string())
    }

    #[rune::function]
    pub fn write(&mut self, channel_name: String, value: rune::Value) -> Result<(), String> {
        let value = value_to_json(value).map_err(|error| error.to_string())?;
        self.deref_mut().write(channel_name, value).map_err(|error| error.to_string())
    }
}

/// Exposé au script sous le nom `next` plutôt que `continue` — mot-clé côté
/// Rune comme côté Rust, la macro `#[rune::function]` ne sait pas mangler
/// l'identifiant brut `r#continue` en nom de fonction interne valide.
#[rune::function]
pub fn next() -> Result<rune::Value, String> {
    frame_result_to_value(FrameResult::Continue)
}

#[rune::function]
pub fn goto(node_id: String) -> Result<rune::Value, String> {
    frame_result_to_value(FrameResult::GoTo(NodeId::from(node_id)))
}

/// Fourche sur plusieurs branches simultanées — `branches` est construit par
/// le script (liste d'objets `{start, overrides}`, voir [`Branch`]) et
/// converti via [`value_to_type`], comme les autres commandes qui reçoivent
/// une structure du domaine plutôt qu'un simple scalaire.
#[rune::function]
pub fn fork(branches: rune::Value, join: String) -> Result<rune::Value, String> {
    let branches: Vec<Branch> = value_to_type(branches)?;
    frame_result_to_value(FrameResult::Fork { branches, join: NodeId::from(join) })
}

/// Contrairement à `next`/`goto`/`fork`/`completed`/`failed` ci-dessus, ne
/// renvoie **pas** un [`FrameResult`] directement : construit la description
/// d'une demande HITL ([`RunLogContent::HitlLog`]), à suspendre
/// explicitement via `yield` côté script (voir la doc de [`Self`] plus
/// haut) — c'est [`GraphScriptEngine::execute`], pas cette fonction, qui
/// décide si la demande doit réellement être posée (première rencontre) ou
/// simplement réinjectée depuis le journal de rejeu (`yield` déjà résolu
/// lors d'un rejeu précédent).
#[rune::function]
pub fn hitl(request: rune::Value) -> Result<rune::Value, String> {
    let request: Hitl = value_to_type(request)?;
    run_log_content_to_value(RunLogContent::HitlLog { request })
}

/// Voir la doc de [`hitl`] — même principe, pour
/// [`RunLogContent::AskExpertLog`].
#[rune::function]
pub fn ask_experts(asks: rune::Value) -> Result<rune::Value, String> {
    let requests: Vec<RequestAskExpert> = value_to_type(asks)?;
    run_log_content_to_value(RunLogContent::ConsultExpertsLog { requests })
}

/// Voir la doc de [`hitl`] — même principe, pour
/// [`RunLogContent::ToolCallLog`].
#[rune::function]
pub fn call_tools(calls: rune::Value) -> Result<rune::Value, String> {
    let requests: Vec<RequestToolCall> = value_to_type(calls)?;
    run_log_content_to_value(RunLogContent::CallToolsLog { requests })
}

/// Voir la doc de [`hitl`] — même principe, pour [`RunLogContent::GraphLog`].
#[rune::function]
pub fn graph(graph_id: String) -> Result<rune::Value, String> {
    run_log_content_to_value(RunLogContent::GraphLog { graph_id: GraphId::from(graph_id) })
}

#[rune::function]
pub fn completed() -> Result<rune::Value, String> {
    frame_result_to_value(FrameResult::Completed)
}

#[rune::function]
pub fn failed(error: String) -> Result<rune::Value, String> {
    frame_result_to_value(FrameResult::Failed(error))
}

/// Convertit une valeur JSON (portée par un canal [`NodeContext`]) en
/// [`rune::Value`] via le pont `serde` de `rune::Value` lui-même
/// (`impl Serialize`/`Deserialize for Value`, voir `rune::runtime::value::serde`) —
/// `serde_json::Value` fait déjà office de `Deserializer` auto-descriptif,
/// pas besoin de reconstruire l'arbre à la main.
fn json_to_value(json: serde_json::Value) -> serde_json::Result<rune::Value> {
    serde_json::from_value(json)
}

/// Sens inverse de [`json_to_value`] — `rune::Value` implémente `Serialize`
/// pour les mêmes raisons.
fn value_to_json(value: rune::Value) -> serde_json::Result<serde_json::Value> {
    serde_json::to_value(value)
}

/// Convertit une valeur construite par le script (objet/tableau Rune) en
/// n'importe quel type `Deserialize` du domaine ([`Hitl`], [`RequestAskExpert`],
/// [`RequestToolCall`], [`Branch`], ...) — même passerelle JSON que
/// [`json_to_value`]/[`value_to_json`], dans l'autre sens, réutilisée par
/// toutes les commandes [`FrameResult`] qui reçoivent une structure plutôt
/// qu'un simple scalaire.
fn value_to_type<T: serde::de::DeserializeOwned>(value: rune::Value) -> Result<T, String> {
    let json = value_to_json(value).map_err(|error| error.to_string())?;
    serde_json::from_value(json).map_err(|error| error.to_string())
}

/// [`FrameResult`] n'est connu que de `serde` (traverse déjà le réseau sous
/// cette forme, voir `session::protocol`) — pas de `#[derive(rune::Any)]`
/// dédié, on le fait transiter par la même passerelle JSON que les canaux
/// plutôt que de doubler sa représentation.
fn frame_result_to_value(result: FrameResult) -> Result<rune::Value, String> {
    let json = serde_json::to_value(result).map_err(|error| error.to_string())?;
    json_to_value(json).map_err(|error| error.to_string())
}

/// Même passerelle que [`frame_result_to_value`], pour la valeur que
/// `hitl`/`ask_experts`/`call_tools`/`graph` renvoient au script — celle-ci
/// n'est jamais la valeur finale de `main`, seulement l'opérande d'un
/// `yield` (voir la doc de [`GraphScriptEngine`]).
fn run_log_content_to_value(content: RunLogContent) -> Result<rune::Value, String> {
    let json = serde_json::to_value(content).map_err(|error| error.to_string())?;
    json_to_value(json).map_err(|error| error.to_string())
}

/// Traduit une demande en attente ([`RunLogContent`], décodée depuis la
/// valeur suspendue par un `yield` non encore résolu — voir
/// [`GraphScriptEngine::execute`]) vers le [`FrameResult`] correspondant,
/// que [`crate::session::controller::SessionHandler::on_frame_run_terminated`]
/// sait traiter (créer le/les frame(s) enfant(s) puis attendre) — même
/// correspondance que les macros Rust `hitl!`/`ask_experts!`/`call_tools!`/
/// `graph!` (voir [`crate::graph::run_log_macros`]).
fn run_log_content_to_frame_result(content: RunLogContent) -> FrameResult {
    match content {
        RunLogContent::HitlLog { request } => FrameResult::RequestHitl(request),
        RunLogContent::ConsultExpertsLog { requests } => FrameResult::ConsultExperts(requests),
        RunLogContent::CallToolsLog { requests } => FrameResult::CallTools(requests),
        RunLogContent::GraphLog { graph_id } => FrameResult::ExecuteGraph(graph_id),
    }
}