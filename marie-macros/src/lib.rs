//! Macros de génération de points d'entrée réseau — RPC (voir [`rpc!`] et
//! [`core_rpc!`]), Job (voir [`job!`] et [`core_job!`]), Tool (voir [`tool!`]
//! et [`core_tool!`]) et Node (voir [`node!`] et [`core_node!`], grammaire à
//! part — voir plus bas).
//!
//! Les six premières (RPC/Job/Tool) partagent la même grammaire :
//!
//! ```ignore
//! rpc! {
//!     #[rpc(name = "my-rpc")] // optionnel, sinon dérivé de `my_rpc` -> "my-rpc"
//!     async fn my_rpc(self: Self<Params>, args: Args) -> Return {
//!         // ...
//!     }
//! }
//! ```
//!
//! `self: Self<...>` déclare l'état porté par le point d'entrée sous deux
//! formes :
//! - `Self<T1, T2, ...>` (tuple) : génère un tuple struct `#struct_name(T1,
//!   T2, ...)`, et `new(field0: T1, field1: T2, ...) -> Self`.
//! - `Self<{n1: T1, n2: T2, ...}>` (champs nommés) : génère un struct à
//!   champs nommés `#struct_name { n1: T1, n2: T2, ... }`, plus un struct
//!   séparé `#struct_nameArgs` (mêmes champs, `#[derive(TypedBuilder)]`) et
//!   `new(args: #struct_nameArgs) -> Self`, pour construire l'état par
//!   builder plutôt que par position — préférable dès que l'état a plus de
//!   deux ou trois champs du même type (où l'ordre positionnel du tuple
//!   struct devient une source d'erreur silencieuse).
//!
//! Dans les deux cas, les champs ne sont portés que sous la feature
//! d'exécuteur correspondante (sans exécuteur local, seuls `NAME`/`Args`/
//! `Return` ont un sens) : `self: Self` (sans paramètre générique) génère un
//! struct sans état dans les deux cas, `Self<>`/`Self<{}>` (vide) sont
//! équivalents à `Self`.
//!
//! Pour `rpc!`/`core_rpc!`, le paramètre `args` peut être suivi d'un
//! troisième paramètre nommé librement pour recevoir le `NodeId` appelant
//! (ex: `caller: NodeId`) ; s'il est omis, l'appelant est ignoré
//! (`_: crate::node::NodeId` pour `core_rpc!`, `_: ::marie::node::NodeId`
//! pour `rpc!`). Pour `job!`/`core_job!` et `tool!`/`core_tool!`,
//! il n'y a pas de troisième paramètre : `execute` renvoie
//! `Result<Return, Error>` plutôt que `Return` directement (le corps doit
//! donc se terminer par une expression de ce type, typiquement `Ok(...)`).
//!
//! `tool!`/`core_tool!` exigent en plus une description (affichée au modèle
//! avec la déclaration du tool) : `#[tool(description = "...")]`, sans quoi
//! la macro échoue à la compilation — il n'y a pas de valeur par défaut
//! sensée, contrairement à `name`.
//!
//! Elles ne diffèrent que par le chemin utilisé pour atteindre le module
//! cible : `core_rpc!`/`core_job!`/`core_tool!` visent `crate::{rpc,job,tools}`
//! (utilisation interne à `marie-core`), `rpc!`/`job!`/`tool!` visent
//! `::marie::{rpc,job,tools}` (utilisation depuis un consommateur externe qui
//! dépend de la façade `marie`).
//!
//! Le code généré implique `#[async_trait::async_trait]` (les traits
//! `RemoteProcedureCall`/`Job`/`Toolable` sont eux-mêmes déclarés avec
//! `#[async_trait]`), pour `rpc!`/`core_rpc!` référence `NodeId` (via
//! `crate_path`), et pour la forme `Self<{...}>` référence
//! `::typed_builder::TypedBuilder` : le
//! crate qui invoque la macro doit donc dépendre directement de
//! `async-trait` (et de `libp2p` pour les RPC, `typed-builder` pour les
//! champs nommés), pas seulement transitivement via `marie`/`marie-core`.
//!
//! `node!`/`core_node!` implémentent [`crate::graph::node::Nodable`]
//! (`marie-core`) — même mécanique de `self: Self<...>` que ci-dessus, mais
//! une grammaire propre plutôt qu'une variation de `Kind` :
//!
//! ```ignore
//! node! {
//!     spec = {
//!         channels = {
//!             "task": { reducer: Reducer::LastWriteWins, inherited: true },
//!             "answer": { reducer: Reducer::LastWriteWins, exported: true },
//!             "ask_id": { reducer: Reducer::LastWriteWins, default: Value::Null }, // serde_json::to_value(...).unwrap()
//!         },
//!     }
//!
//!     #[node(name = "ask-expert")] // optionnel, sinon dérivé de `ask_expert` -> "ask-expert"
//!     pub async fn ask_expert(self: Self<{ expert: ExpertId }>, ctx: NodeContext) -> crate::Result<FrameResult> {
//!         // ...
//!     }
//! }
//! ```
//!
//! Le bloc `spec = { ... }` (obligatoire, pas de valeur par défaut sensée —
//! même raisonnement que la description de `tool!`) précède chaque entrée et
//! devient son `Nodable::common_spec`, via la même grammaire que
//! [`spec!`]/[`core_spec!`] ci-dessous. Chaque canal du bloc `channels = {
//! "nom": { ... }, ... }` porte son propre `reducer` (obligatoire — devient
//! l'entrée correspondante de `CommonSpec::channels`) et cinq attributs
//! individuellement optionnels : `inherited: true` (ajoute le nom du canal à
//! `inherited_channels`, reçu de l'ancêtre à la création du frame),
//! `exported: true` (ajoute le nom du canal à `exported_channels`, poussé
//! vers le parent quand ce frame termine), `imported: true` (ajoute le nom du
//! canal à `imported_channels`, symétrique côté parent : seuls les canaux
//! listés ici sont acceptés parmi ceux qu'exportent les enfants terminés —
//! voir `SessionHandler::drain_pending_accumulators`), `default: <expr>`
//! (insère `("nom", expression passée à serde_json::to_value(...).unwrap())`
//! dans `default_values`) et `overrides: <expr>` (même transformation, dans
//! `overrides_channels`) — plus besoin de répéter le nom du canal dans des
//! listes séparées, tout se
//! déclare au même endroit que le `reducer`. Un bloc `budget = { max_step:
//! <expr>, max_tokens: <expr> }` optionnel précède `channels` — ses deux
//! champs sont eux-mêmes optionnels (`None` par défaut, comme sur
//! `Budget`). Le second paramètre de la fonction reçoit toujours le contexte
//! de la node : il peut être déclaré `ctx: NodeContext` (la macro insère
//! `&mut` elle-même, `Nodable::execute` le reçoit par référence mutable) ou
//! directement `ctx: &mut NodeContext`. Il n'y a ni troisième paramètre ni
//! `Args`/`Return` associés — le type de retour écrit après `->` est utilisé
//! tel quel comme retour d'`execute` (donc `crate::Result<FrameResult>`,
//! jamais une autre forme). `core_node!` vise `crate::graph::node`
//! (utilisation interne à `marie-core`), `node!` vise `::marie::graph::node`
//! (façade externe).
//!
//! [`spec!`]/[`core_spec!`] génèrent directement l'expression `CommonSpec {
//! ... }` correspondant au bloc `spec = { ... }` ci-dessus, sans node autour
//! — pour construire une `CommonSpec` de façon autonome (ex: les
//! constructeurs `CommonSpec::tool`/`expert`/`hitl` de `marie-core`) :
//!
//! ```ignore
//! let spec = spec! {
//!     budget = { max_step: 32, max_tokens: 1024 },
//!     channels = {
//!         "task": { reducer: Reducer::LastWriteWins, inherited: true },
//!         "answer": { reducer: Reducer::LastWriteWins, exported: true, overrides: 32 },
//!     }
//! };
//! ```
//!
//! `core_spec!` vise `crate::session::spec` (utilisation interne à
//! `marie-core`), `spec!` vise `::marie::session::spec` (façade externe).

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Attribute, Block, Expr, Ident, Lit, LitStr, Pat, Token, Type, Visibility,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
};

/// État porté par le point d'entrée, déclaré via `self: Self<...>` — voir la
/// documentation de module.
enum SelfFields {
    /// `self: Self` (ou `Self<>`) : pas d'état.
    Unit,
    /// `self: Self<T1, T2, ...>` : tuple struct.
    Tuple(Vec<Type>),
    /// `self: Self<{n1: T1, n2: T2, ...}>` : struct à champs nommés + Args
    /// builder.
    Named(Vec<(Ident, Type)>),
}

/// Un champ `nom: Type` à l'intérieur de `Self<{...}>`.
struct NamedField {
    name: Ident,
    ty: Type,
}

impl Parse for NamedField {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        let ty: Type = input.parse()?;
        Ok(NamedField { name, ty })
    }
}

/// Parse la partie `<...>` (optionnelle) de `self: Self<...>` — factorisé
/// entre [`Item::parse`] (`rpc!`/`job!`/`tool!`) et `NodeItem::parse`
/// (`node!`), qui partagent exactement cette grammaire.
fn parse_self_fields(content: ParseStream) -> syn::Result<SelfFields> {
    let self_fields = if content.peek(Token![<]) {
        content.parse::<Token![<]>()?;

        let fields = if content.peek(syn::token::Brace) {
            let braced;
            syn::braced!(braced in content);
            let named = Punctuated::<NamedField, Token![,]>::parse_terminated(&braced)?;
            SelfFields::Named(named.into_iter().map(|f| (f.name, f.ty)).collect())
        } else {
            let mut types = Vec::new();
            while !content.peek(Token![>]) {
                types.push(content.parse::<Type>()?);
                if content.peek(Token![,]) {
                    content.parse::<Token![,]>()?;
                } else {
                    break;
                }
            }
            SelfFields::Tuple(types)
        };

        content.parse::<Token![>]>()?;
        fields
    } else {
        SelfFields::Unit
    };
    Ok(self_fields)
}

/// Une entrée `async fn ...(self: Self<...>, args: Args[, third: Third]) ->
/// Return { body }` — syntaxe volontairement pas celle d'une vraie fonction
/// Rust (la forme `Self<{...}>` n'en est pas une), d'où un parseur dédié
/// plutôt qu'une délégation à `syn::ItemFn`.
struct Item {
    attrs: Vec<Attribute>,
    fn_name: Ident,
    self_fields: SelfFields,
    args_pat: Pat,
    args_ty: Type,
    third: Option<(Pat, Type)>,
    return_ty: Type,
    body: Block,
}

impl Parse for Item {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        let _vis: Visibility = input.parse()?;
        input.parse::<Token![async]>()?;
        input.parse::<Token![fn]>()?;
        let fn_name: Ident = input.parse()?;

        let content;
        syn::parenthesized!(content in input);

        content.parse::<Token![self]>()?;
        content.parse::<Token![:]>()?;
        content.parse::<Token![Self]>().map_err(|_| {
            syn::Error::new(content.span(), "le type de `self` doit être `Self`")
        })?;

        let self_fields = parse_self_fields(&content)?;

        content.parse::<Token![,]>().map_err(|_| {
            syn::Error::new(
                content.span(),
                "paramètre `args` manquant : `fn ..(self: Self<..>, args: Args) -> Return`",
            )
        })?;
        let args_pat: Pat = content.call(Pat::parse_single)?;
        content.parse::<Token![:]>()?;
        let args_ty: Type = content.parse()?;

        let third = if content.peek(Token![,]) {
            content.parse::<Token![,]>()?;
            let pat: Pat = content.call(Pat::parse_single)?;
            content.parse::<Token![:]>()?;
            let ty: Type = content.parse()?;
            Some((pat, ty))
        } else {
            None
        };

        if !content.is_empty() {
            return Err(content.error("trop de paramètres pour cette macro"));
        }

        input.parse::<Token![->]>().map_err(|_| {
            syn::Error::new(input.span(), "un type de retour est requis : `-> Return`")
        })?;
        let return_ty: Type = input.parse()?;
        let body: Block = input.parse()?;

        Ok(Item { attrs, fn_name, self_fields, args_pat, args_ty, third, return_ty, body })
    }
}

struct Items(Vec<Item>);

impl Parse for Items {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut items = Vec::new();
        while !input.is_empty() {
            items.push(input.parse()?);
        }
        Ok(Items(items))
    }
}

/// Une entrée `"nom": { reducer: <expr>, inherited: bool, exported: bool,
/// imported: bool, default: <expr>, overrides: <expr> }` dans le bloc
/// `channels = { ... }` de `spec!` — voir [`SpecBlock`]. Seul `reducer` est
/// obligatoire ; `inherited`/`exported`/`imported` (défaut `false`) et
/// `default`/`overrides` (défaut absent) portent, directement sur le canal,
/// ce qui relevait auparavant de listes séparées (`exported = [...]`/
/// `inherited = [...]`/`default = { ... }`) au niveau du bloc `spec`.
struct SpecChannel {
    name: LitStr,
    reducer: Expr,
    inherited: bool,
    exported: bool,
    imported: bool,
    default: Option<Expr>,
    overrides: Option<Expr>,
}

impl Parse for SpecChannel {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: LitStr = input.parse()?;
        input.parse::<Token![:]>()?;

        let content;
        syn::braced!(content in input);

        let mut reducer = None;
        let mut inherited = false;
        let mut exported = false;
        let mut imported = false;
        let mut default = None;
        let mut overrides = None;
        let mut seen = std::collections::HashSet::new();

        while !content.is_empty() {
            let key: Ident = content.parse()?;
            let key_str = key.to_string();
            if !seen.insert(key_str.clone()) {
                return Err(syn::Error::new(key.span(), format!("champ `{key_str}` déjà défini")));
            }
            content.parse::<Token![:]>()?;

            match key_str.as_str() {
                "reducer" => {
                    reducer = Some(content.parse::<Expr>()?);
                }
                "inherited" => {
                    inherited = content.parse::<syn::LitBool>()?.value;
                }
                "exported" => {
                    exported = content.parse::<syn::LitBool>()?.value;
                }
                "imported" => {
                    imported = content.parse::<syn::LitBool>()?.value;
                }
                "default" => {
                    default = Some(content.parse::<Expr>()?);
                }
                "overrides" => {
                    overrides = Some(content.parse::<Expr>()?);
                }
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("champ `{other}` inconnu pour un canal, attendu `reducer`/`inherited`/`exported`/`imported`/`default`/`overrides`"),
                    ));
                }
            }

            if content.peek(Token![,]) {
                content.parse::<Token![,]>()?;
            }
        }

        let reducer = reducer.ok_or_else(|| {
            syn::Error::new(name.span(), "le champ `reducer` est requis pour chaque canal")
        })?;

        Ok(SpecChannel { name, reducer, inherited, exported, imported, default, overrides })
    }
}

/// Le bloc `budget = { max_step: <expr>, max_tokens: <expr> }` optionnel du
/// bloc `spec`/`spec!` — les deux champs sont eux-mêmes optionnels (`None`
/// par défaut, comme sur [`crate::session::spec::Budget`] côté `marie-core`).
#[derive(Default)]
struct SpecBudget {
    max_step: Option<Expr>,
    max_tokens: Option<Expr>,
}

impl Parse for SpecBudget {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut budget = SpecBudget::default();
        let mut seen = std::collections::HashSet::new();

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            let key_str = key.to_string();
            if !seen.insert(key_str.clone()) {
                return Err(syn::Error::new(key.span(), format!("champ `{key_str}` déjà défini")));
            }
            input.parse::<Token![:]>()?;

            match key_str.as_str() {
                "max_step" => {
                    budget.max_step = Some(input.parse::<Expr>()?);
                }
                "max_tokens" => {
                    budget.max_tokens = Some(input.parse::<Expr>()?);
                }
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("champ `{other}` inconnu pour `budget`, attendu `max_step`/`max_tokens`"),
                    ));
                }
            }

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(budget)
    }
}

/// Le bloc `spec = { budget = { ... }, channels = { "nom": { reducer: ...,
/// ... }, ... } }` requis avant chaque entrée de `node!`/`core_node!` (voir
/// [`NodeItem::parse`]) — transformé en corps de `Nodable::common_spec`. Même
/// grammaire que celle acceptée directement par [`spec!`]/[`core_spec!`],
/// qui produit l'expression `CommonSpec { ... }` correspondante sans passer
/// par une node. `budget`/`channels` sont tous deux optionnels (une spec vide
/// est valide), mais pour `node!`/`core_node!` le bloc `spec = { ... }`
/// lui-même reste obligatoire : il n'y a pas de valeur par défaut sensée pour
/// la spec d'une node, comme pour `description` sur `tool!`/`core_tool!`.
#[derive(Default)]
struct SpecBlock {
    budget: Option<SpecBudget>,
    channels: Vec<SpecChannel>,
}

impl Parse for SpecBlock {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut spec = SpecBlock::default();
        let mut seen = std::collections::HashSet::new();

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            let key_str = key.to_string();
            if !seen.insert(key_str.clone()) {
                return Err(syn::Error::new(key.span(), format!("champ `{key_str}` déjà défini")));
            }
            input.parse::<Token![=]>()?;

            match key_str.as_str() {
                "budget" => {
                    let content;
                    syn::braced!(content in input);
                    spec.budget = Some(content.parse::<SpecBudget>()?);
                }
                "channels" => {
                    let content;
                    syn::braced!(content in input);
                    spec.channels = Punctuated::<SpecChannel, Token![,]>::parse_terminated(&content)?
                        .into_iter()
                        .collect();
                }
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("champ `{other}` inconnu dans `spec`, attendu `budget`/`channels`"),
                    ));
                }
            }

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(spec)
    }
}

/// Génère le corps de `Nodable::common_spec` (via `spec = { ... }` dans
/// `node!`/`core_node!`) ou l'expression produite par [`spec!`]/[`core_spec!`]
/// directement, à partir de [`SpecBlock`] — équivalent du `CommonSpec { ... }`
/// écrit à la main dans `graph::builtin`.
fn build_common_spec(spec: &SpecBlock, crate_path: &TokenStream2) -> TokenStream2 {
    let channels = spec.channels.iter().map(|c| {
        let name = &c.name;
        let reducer = &c.reducer;
        quote!(#crate_path::session::channel::ChannelSpec::new(#name, #reducer))
    });
    let inherited = spec.channels.iter().filter(|c| c.inherited).map(|c| {
        let name = &c.name;
        quote!(#crate_path::session::channel::ChannelName::from(#name))
    });
    let exported = spec.channels.iter().filter(|c| c.exported).map(|c| {
        let name = &c.name;
        quote!(#crate_path::session::channel::ChannelName::from(#name))
    });
    let imported = spec.channels.iter().filter(|c| c.imported).map(|c| {
        let name = &c.name;
        quote!(#crate_path::session::channel::ChannelName::from(#name))
    });
    let defaults = spec.channels.iter().filter_map(|c| {
        let default = c.default.as_ref()?;
        let name = &c.name;
        Some(quote!((#crate_path::session::channel::ChannelName::from(#name), ::serde_json::to_value(#default).unwrap())))
    });
    let overrides = spec.channels.iter().filter_map(|c| {
        let overrides = c.overrides.as_ref()?;
        let name = &c.name;
        Some(quote!((#crate_path::session::channel::ChannelName::from(#name), ::serde_json::to_value(#overrides).unwrap())))
    });

    let budget = match &spec.budget {
        Some(budget) => {
            let max_step = match &budget.max_step {
                Some(expr) => quote!(::std::option::Option::Some(#expr)),
                None => quote!(::std::option::Option::None),
            };
            let max_tokens = match &budget.max_tokens {
                Some(expr) => quote!(::std::option::Option::Some(#expr)),
                None => quote!(::std::option::Option::None),
            };
            quote!(#crate_path::session::spec::Budget { max_step: #max_step, max_tokens: #max_tokens })
        }
        None => quote!(::std::default::Default::default()),
    };

    quote! {
        #crate_path::session::spec::CommonSpec {
            budget: #budget,
            channels: vec![#(#channels),*],
            inherited_channels: vec![#(#inherited),*],
            exported_channels: vec![#(#exported),*],
            imported_channels: vec![#(#imported),*],
            default_values: ::std::collections::HashMap::from([#(#defaults),*]),
            overrides_channels: ::std::collections::HashMap::from([#(#overrides),*]),
        }
    }
}

/// Une entrée `spec = { ... } #[node(name = "...")] async fn ...(self:
/// Self<...>, ctx: NodeContext) -> Return { body }` de `node!`/`core_node!` —
/// voir la documentation de module. Contrairement à [`Item`]
/// (`rpc!`/`job!`/`tool!`), le second paramètre n'est pas un `Args` libre
/// mais toujours `NodeContext` (le corps de `Nodable::execute` le reçoit par
/// référence mutable : si l'appelant écrit le type par valeur — comme dans
/// l'exemple ci-dessus —, la macro ajoute `&mut` elle-même plutôt que
/// d'exiger que chaque déclaration la répète), et le type de retour n'a pas
/// de forme `Result<Return, Error>` à déduire : `Nodable::execute` fixe déjà
/// son retour à `crate::Result<FrameResult>`, donc le type écrit après `->`
/// est utilisé tel quel.
struct NodeItem {
    spec: SpecBlock,
    attrs: Vec<Attribute>,
    fn_name: Ident,
    self_fields: SelfFields,
    ctx_pat: Pat,
    ctx_ty: Type,
    return_ty: Type,
    body: Block,
}

impl Parse for NodeItem {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let spec_kw: Ident = input.parse().map_err(|_| {
            syn::Error::new(input.span(), "un bloc `spec = { ... }` est requis avant chaque node")
        })?;
        if spec_kw != "spec" {
            return Err(syn::Error::new(
                spec_kw.span(),
                "un bloc `spec = { ... }` est requis avant chaque node",
            ));
        }
        input.parse::<Token![=]>()?;
        let spec_content;
        syn::braced!(spec_content in input);
        let spec: SpecBlock = spec_content.parse()?;

        let attrs = input.call(Attribute::parse_outer)?;
        let _vis: Visibility = input.parse()?;
        input.parse::<Token![async]>()?;
        input.parse::<Token![fn]>()?;
        let fn_name: Ident = input.parse()?;

        let content;
        syn::parenthesized!(content in input);

        content.parse::<Token![self]>()?;
        content.parse::<Token![:]>()?;
        content.parse::<Token![Self]>().map_err(|_| {
            syn::Error::new(content.span(), "le type de `self` doit être `Self`")
        })?;

        let self_fields = parse_self_fields(&content)?;

        content.parse::<Token![,]>().map_err(|_| {
            syn::Error::new(
                content.span(),
                "paramètre `ctx` manquant : `fn ..(self: Self<..>, ctx: NodeContext) -> Return`",
            )
        })?;
        let ctx_pat: Pat = content.call(Pat::parse_single)?;
        content.parse::<Token![:]>()?;
        let ctx_ty: Type = content.parse()?;

        if !content.is_empty() {
            return Err(content.error("trop de paramètres pour cette macro"));
        }

        input.parse::<Token![->]>().map_err(|_| {
            syn::Error::new(input.span(), "un type de retour est requis : `-> Return`")
        })?;
        let return_ty: Type = input.parse()?;
        let body: Block = input.parse()?;

        Ok(NodeItem { spec, attrs, fn_name, self_fields, ctx_pat, ctx_ty, return_ty, body })
    }
}

struct NodeItems(Vec<NodeItem>);

impl Parse for NodeItems {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut items = Vec::new();
        while !input.is_empty() {
            items.push(input.parse()?);
        }
        Ok(NodeItems(items))
    }
}

/// Paramètres qui distinguent `rpc!`/`core_rpc!` de `job!`/`core_job!` de
/// `tool!`/`core_tool!` — voir la documentation de module.
struct Kind {
    /// Nom de l'attribut de configuration reconnu sur la fonction (ex:
    /// `#[rpc(name = "...")]` vs `#[job(name = "...")]`).
    attr_name: &'static str,
    /// Feature sous laquelle le struct porte ses champs d'état et sous
    /// laquelle `execute` est généré (ex: `rpc-executor`/`job-executor`).
    feature: &'static str,
    /// Chemin du trait à implémenter, relatif à `crate_path` (ex:
    /// `rpc::RemoteProcedureCall` / `job::Job`).
    trait_path: TokenStream2,
    /// Troisième paramètre optionnel après `args` (ex: `caller: NodeId` pour
    /// les RPC, résolu via `crate_path` en `crate::node::NodeId` pour
    /// `core_rpc!` ou `::marie::node::NodeId` pour `rpc!`) ; `false` pour les
    /// Job/Tool, qui n'en ont pas.
    has_default_third_param: bool,
    /// Si `true`, `execute` renvoie `Result<Return, Error>` (Job/Tool) plutôt
    /// que `Return` directement (RPC).
    wrap_return_in_result: bool,
    /// Si `true`, exige `#[<attr_name>(description = "...")]` sur la
    /// fonction et génère `const DESCRIPTION: &str = "..."` dans l'impl
    /// (Tool uniquement — `Toolable::DESCRIPTION`, affichée au modèle).
    requires_description: bool,
}

/// Génère un ou plusieurs RPC référençant `::marie::rpc` — pour les
/// consommateurs externes qui dépendent de la façade `marie`. Voir la
/// documentation de module pour la grammaire complète.
#[proc_macro]
pub fn rpc(input: TokenStream) -> TokenStream {
    expand(input, quote!(::marie), rpc_kind())
}

/// Génère un ou plusieurs RPC référençant `crate::rpc` — pour une utilisation
/// interne à `marie-core`. Voir la documentation de module pour la
/// grammaire complète.
#[proc_macro]
pub fn core_rpc(input: TokenStream) -> TokenStream {
    expand(input, quote!(crate), rpc_kind())
}

/// Génère un ou plusieurs Job référençant `::marie::job` — pour les
/// consommateurs externes qui dépendent de la façade `marie`. Voir la
/// documentation de module pour la grammaire complète.
#[proc_macro]
pub fn job(input: TokenStream) -> TokenStream {
    expand(input, quote!(::marie), job_kind())
}

/// Génère un ou plusieurs Job référençant `crate::job` — pour une utilisation
/// interne à `marie-core`. Voir la documentation de module pour la grammaire
/// complète.
#[proc_macro]
pub fn core_job(input: TokenStream) -> TokenStream {
    expand(input, quote!(crate), job_kind())
}

/// Génère un ou plusieurs Tool référençant `::marie::tools` — pour les
/// consommateurs externes qui dépendent de la façade `marie`. Voir la
/// documentation de module pour la grammaire complète (`description`
/// obligatoire en plus de `name`).
#[proc_macro]
pub fn tool(input: TokenStream) -> TokenStream {
    expand(input, quote!(::marie), tool_kind())
}

/// Génère un ou plusieurs Tool référençant `crate::tools` — pour une
/// utilisation interne à `marie-core`. Voir la documentation de module pour
/// la grammaire complète (`description` obligatoire en plus de `name`).
#[proc_macro]
pub fn core_tool(input: TokenStream) -> TokenStream {
    expand(input, quote!(crate), tool_kind())
}

/// Génère une ou plusieurs Node référençant `::marie::graph::node` — pour les
/// consommateurs externes qui dépendent de la façade `marie`. Grammaire
/// propre à `node!`/`core_node!`, distincte de `rpc!`/`job!`/`tool!` — voir
/// la documentation de module.
#[proc_macro]
pub fn node(input: TokenStream) -> TokenStream {
    expand_node(input, quote!(::marie))
}

/// Génère une ou plusieurs Node référençant `crate::graph::node` — pour une
/// utilisation interne à `marie-core`. Grammaire propre à
/// `node!`/`core_node!` — voir la documentation de module.
#[proc_macro]
pub fn core_node(input: TokenStream) -> TokenStream {
    expand_node(input, quote!(crate))
}

/// Génère directement une expression `CommonSpec { ... }` référençant
/// `::marie::session::spec` — même grammaire que le bloc `spec = { ... }` de
/// `node!`/`core_node!` (voir [`SpecBlock`]), mais sans node autour : utile
/// pour construire une [`CommonSpec`](crate::session::spec::CommonSpec)
/// autonome (ex: `session::spec::CommonSpec::tool`/`expert`/`hitl`).
///
/// ```ignore
/// let spec = spec! {
///     budget = { max_step: 32, max_tokens: 1024 },
///     channels = {
///         "task": { reducer: Reducer::LastWriteWins, inherited: true },
///         "answer": { reducer: Reducer::LastWriteWins, exported: true, overrides: 32 },
///     }
/// };
/// ```
#[proc_macro]
pub fn spec(input: TokenStream) -> TokenStream {
    expand_spec(input, quote!(::marie))
}

/// Génère directement une expression `CommonSpec { ... }` référençant
/// `crate::session::spec` — pour une utilisation interne à `marie-core`. Voir
/// [`spec!`] pour la grammaire complète.
#[proc_macro]
pub fn core_spec(input: TokenStream) -> TokenStream {
    expand_spec(input, quote!(crate))
}

fn rpc_kind() -> Kind {
    Kind {
        attr_name: "rpc",
        feature: "rpc-executor",
        trait_path: quote!(rpc::RemoteProcedureCall),
        has_default_third_param: true,
        wrap_return_in_result: false,
        requires_description: false,
    }
}

fn job_kind() -> Kind {
    Kind {
        attr_name: "job",
        feature: "job-executor",
        trait_path: quote!(job::Job),
        has_default_third_param: false,
        wrap_return_in_result: true,
        requires_description: false,
    }
}

fn tool_kind() -> Kind {
    Kind {
        attr_name: "tool",
        feature: "tool-executor",
        trait_path: quote!(tools::Toolable),
        has_default_third_param: false,
        wrap_return_in_result: true,
        requires_description: true,
    }
}

fn expand(input: TokenStream, crate_path: TokenStream2, kind: Kind) -> TokenStream {
    let items = parse_macro_input!(input as Items).0;

    let mut out = TokenStream2::new();
    for item in items {
        match expand_one(item, &crate_path, &kind) {
            Ok(ts) => out.extend(ts),
            Err(err) => out.extend(err.to_compile_error()),
        }
    }
    out.into()
}

fn expand_node(input: TokenStream, crate_path: TokenStream2) -> TokenStream {
    let items = parse_macro_input!(input as NodeItems).0;

    let mut out = TokenStream2::new();
    for item in items {
        match expand_node_one(item, &crate_path) {
            Ok(ts) => out.extend(ts),
            Err(err) => out.extend(err.to_compile_error()),
        }
    }
    out.into()
}

/// Parse `input` directement comme un [`SpecBlock`] (pas de bloc `spec =
/// { ... }` autour, contrairement à [`NodeItem::parse`] : ici c'est
/// l'intégralité de l'invocation) et l'expand en l'expression `CommonSpec {
/// ... }` correspondante — voir [`spec!`]/[`core_spec!`].
fn expand_spec(input: TokenStream, crate_path: TokenStream2) -> TokenStream {
    let spec = parse_macro_input!(input as SpecBlock);
    build_common_spec(&spec, &crate_path).into()
}

fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

fn expand_one(item: Item, crate_path: &TokenStream2, kind: &Kind) -> syn::Result<TokenStream2> {
    let mut name_override = None;
    let mut description = None;
    let mut doc_attrs: Vec<Attribute> = Vec::new();

    for attr in &item.attrs {
        if attr.path().is_ident(kind.attr_name) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    let value = meta.value()?;
                    let lit: Lit = value.parse()?;
                    let Lit::Str(lit) = lit else {
                        return Err(meta.error("`name` attend une chaîne littérale"));
                    };
                    name_override = Some(lit.value());
                    Ok(())
                } else if kind.requires_description && meta.path.is_ident("description") {
                    let value = meta.value()?;
                    let lit: Lit = value.parse()?;
                    let Lit::Str(lit) = lit else {
                        return Err(meta.error("`description` attend une chaîne littérale"));
                    };
                    description = Some(lit.value());
                    Ok(())
                } else if kind.requires_description {
                    Err(meta.error(format!("attribut `{}` inconnu, seuls `name`/`description` sont supportés", kind.attr_name)))
                } else {
                    Err(meta.error(format!("attribut `{}` inconnu, seul `name` est supporté", kind.attr_name)))
                }
            })?;
        } else if attr.path().is_ident("doc") {
            doc_attrs.push(attr.clone());
        } else if kind.requires_description {
            return Err(syn::Error::new_spanned(
                attr,
                format!("seul l'attribut `#[{}(name = \"...\", description = \"...\")]` est supporté ici", kind.attr_name),
            ));
        } else {
            return Err(syn::Error::new_spanned(
                attr,
                format!("seul l'attribut `#[{}(name = \"...\")]` est supporté ici", kind.attr_name),
            ));
        }
    }

    if kind.requires_description && description.is_none() {
        return Err(syn::Error::new_spanned(
            &item.fn_name,
            format!("une description est requise : `#[{}(description = \"...\")]`", kind.attr_name),
        ));
    }

    let fn_name = &item.fn_name;
    let struct_name = format_ident!("{}", to_pascal_case(&fn_name.to_string()), span = fn_name.span());

    let name_value = name_override.unwrap_or_else(|| fn_name.to_string().replace('_', "-"));
    let feature = kind.feature;

    let (executor_struct, non_executor_struct, args_struct, executor_new, non_executor_new) =
        build_self_fields_code(&struct_name, &item.self_fields, feature, &doc_attrs);

    let args_pat = &item.args_pat;
    let args_ty = &item.args_ty;
    let return_ty = &item.return_ty;
    let body = &item.body;

    let trait_path = &kind.trait_path;
    let execute_params = match &item.third {
        Some((pat, ty)) => quote!(#args_pat: #args_ty, #pat: #ty),
        None => if kind.has_default_third_param {
            quote!(#args_pat: #args_ty, _: #crate_path::node::NodeId)
        } else {
            quote!(#args_pat: #args_ty)
        },
    };
    let execute_return_ty = if kind.wrap_return_in_result {
        quote!(#crate_path::Result<#return_ty>)
    } else {
        quote!(#return_ty)
    };

    let description_const = description.map(|description| quote!(const DESCRIPTION: &'static str = #description;));

    Ok(quote! {
        #executor_struct

        #non_executor_struct

        #args_struct

        #executor_new

        #non_executor_new

        #[async_trait::async_trait]
        impl #crate_path::#trait_path for #struct_name {
            const NAME: &'static str = #name_value;
            #description_const

            type Args = #args_ty;
            type Return = #return_ty;

            #[cfg(feature = #feature)]
            async fn execute(self, #execute_params) -> #execute_return_ty #body
        }
    })
}

const NODE_FEATURE: &str = "node-executor";

fn expand_node_one(item: NodeItem, crate_path: &TokenStream2) -> syn::Result<TokenStream2> {
    let mut name_override = None;
    let mut doc_attrs: Vec<Attribute> = Vec::new();

    for attr in &item.attrs {
        if attr.path().is_ident("node") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    let value = meta.value()?;
                    let lit: Lit = value.parse()?;
                    let Lit::Str(lit) = lit else {
                        return Err(meta.error("`name` attend une chaîne littérale"));
                    };
                    name_override = Some(lit.value());
                    Ok(())
                } else {
                    Err(meta.error("attribut `node` inconnu, seul `name` est supporté"))
                }
            })?;
        } else if attr.path().is_ident("doc") {
            doc_attrs.push(attr.clone());
        } else {
            return Err(syn::Error::new_spanned(
                attr,
                "seul l'attribut `#[node(name = \"...\")]` est supporté ici",
            ));
        }
    }

    let fn_name = &item.fn_name;
    let struct_name = format_ident!("{}", to_pascal_case(&fn_name.to_string()), span = fn_name.span());
    let name_value = name_override.unwrap_or_else(|| fn_name.to_string().replace('_', "-"));

    let (executor_struct, non_executor_struct, args_struct, executor_new, non_executor_new) =
        build_self_fields_code(&struct_name, &item.self_fields, NODE_FEATURE, &doc_attrs);

    let common_spec_body = build_common_spec(&item.spec, crate_path);

    let ctx_pat = &item.ctx_pat;
    let ctx_ty = &item.ctx_ty;
    let ctx_ty_tokens = match ctx_ty {
        Type::Reference(_) => quote!(#ctx_ty),
        _ => quote!(&mut #ctx_ty),
    };
    let return_ty = &item.return_ty;
    let body = &item.body;

    Ok(quote! {
        #executor_struct

        #non_executor_struct

        #args_struct

        #executor_new

        #non_executor_new

        #[async_trait::async_trait]
        impl #crate_path::graph::node::Nodable for #struct_name {
            const NAME: &'static str = #name_value;

            fn common_spec() -> #crate_path::session::spec::CommonSpec {
                #common_spec_body
            }

            #[cfg(feature = #NODE_FEATURE)]
            async fn execute(self, #ctx_pat: #ctx_ty_tokens) -> #return_ty #body
        }
    })
}

/// Génère les structs (exécuteur/non-exécuteur), le struct `Args` optionnel
/// (forme `Self<{...}>` uniquement) et les `impl ... { fn new(...) }`
/// associés à `self: Self<...>` — factorisé entre `expand_one`
/// (`rpc!`/`job!`/`tool!`) et `expand_node_one` (`node!`), qui partagent
/// exactement cette génération, indépendamment du trait implémenté.
fn build_self_fields_code(
    struct_name: &Ident,
    self_fields: &SelfFields,
    feature: &'static str,
    doc_attrs: &[Attribute],
) -> (TokenStream2, TokenStream2, Option<TokenStream2>, TokenStream2, TokenStream2) {
    match self_fields {
        SelfFields::Unit => {
            let executor_struct = quote! {
                #(#doc_attrs)*
                #[cfg(feature = #feature)]
                #[derive(Clone)]
                pub struct #struct_name;
            };
            let non_executor_struct = quote! {
                #(#doc_attrs)*
                #[cfg(not(feature = #feature))]
                #[derive(Clone)]
                pub struct #struct_name;
            };
            let executor_new = build_tuple_new(struct_name, &[], quote!(feature = #feature));
            let non_executor_new = build_tuple_new(struct_name, &[], quote!(not(feature = #feature)));
            (executor_struct, non_executor_struct, None, executor_new, non_executor_new)
        }
        SelfFields::Tuple(field_types) => {
            let executor_struct = if field_types.is_empty() {
                quote! {
                    #(#doc_attrs)*
                    #[cfg(feature = #feature)]
                    #[derive(Clone)]
                    pub struct #struct_name;
                }
            } else {
                quote! {
                    #(#doc_attrs)*
                    #[cfg(feature = #feature)]
                    #[derive(Clone)]
                    pub struct #struct_name(#(#field_types),*);
                }
            };
            let non_executor_struct = quote! {
                #(#doc_attrs)*
                #[cfg(not(feature = #feature))]
                #[derive(Clone)]
                pub struct #struct_name;
            };
            let executor_new = build_tuple_new(struct_name, field_types, quote!(feature = #feature));
            let non_executor_new = build_tuple_new(struct_name, &[], quote!(not(feature = #feature)));
            (executor_struct, non_executor_struct, None, executor_new, non_executor_new)
        }
        SelfFields::Named(fields) => {
            let args_struct_name = format_ident!("{}Params", struct_name, span = struct_name.span());
            let field_names: Vec<&Ident> = fields.iter().map(|(name, _)| name).collect();
            let field_types: Vec<&Type> = fields.iter().map(|(_, ty)| ty).collect();

            let executor_struct = quote! {
                #(#doc_attrs)*
                #[cfg(feature = #feature)]
                #[derive(Clone)]
                pub struct #struct_name {
                    #(#field_names: #field_types),*
                }
            };
            let non_executor_struct = quote! {
                #(#doc_attrs)*
                #[cfg(not(feature = #feature))]
                #[derive(Clone)]
                pub struct #struct_name;
            };
            let args_struct = quote! {
                #[cfg(feature = #feature)]
                #[derive(::typed_builder::TypedBuilder)]
                pub struct #args_struct_name {
                    #(#field_names: #field_types),*
                }
            };
            let executor_new = quote! {
                #[cfg(feature = #feature)]
                impl #struct_name {
                    pub fn new(args: #args_struct_name) -> Self {
                        Self { #(#field_names: args.#field_names),* }
                    }
                }
            };
            let non_executor_new = build_tuple_new(struct_name, &[], quote!(not(feature = #feature)));
            (executor_struct, non_executor_struct, Some(args_struct), executor_new, non_executor_new)
        }
    }
}

/// Génère `impl #struct_name { pub fn new(...) -> Self { ... } }` pour un
/// tuple struct (ou un struct unité si `field_types` est vide), sous le
/// `cfg` donné.
fn build_tuple_new(struct_name: &Ident, field_types: &[Type], cfg: TokenStream2) -> TokenStream2 {
    if field_types.is_empty() {
        quote! {
            #[cfg(#cfg)]
            impl #struct_name {
                pub fn new() -> Self {
                    Self
                }
            }
        }
    } else {
        let params: Vec<Ident> = (0..field_types.len())
            .map(|i| format_ident!("field{}", i, span = struct_name.span()))
            .collect();
        quote! {
            #[cfg(#cfg)]
            impl #struct_name {
                pub fn new(#(#params: #field_types),*) -> Self {
                    Self(#(#params),*)
                }
            }
        }
    }
}
