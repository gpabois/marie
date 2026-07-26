//! Macros de génération de points d'entrée réseau — RPC (voir [`rpc!`] et
//! [`core_rpc!`]), Job (voir [`job!`] et [`core_job!`]) et Tool (voir
//! [`tool!`] et [`core_tool!`]).
//!
//! Les six macros partagent la même grammaire :
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

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Attribute, Block, Ident, Lit, Pat, Token, Type, Visibility,
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

    let (executor_struct, non_executor_struct, args_struct, executor_new, non_executor_new) = match &item.self_fields {
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
            let executor_new = build_tuple_new(&struct_name, &[], quote!(feature = #feature));
            let non_executor_new = build_tuple_new(&struct_name, &[], quote!(not(feature = #feature)));
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
            let executor_new = build_tuple_new(&struct_name, field_types, quote!(feature = #feature));
            let non_executor_new = build_tuple_new(&struct_name, &[], quote!(not(feature = #feature)));
            (executor_struct, non_executor_struct, None, executor_new, non_executor_new)
        }
        SelfFields::Named(fields) => {
            let args_struct_name = format_ident!("{}Args", struct_name, span = struct_name.span());
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
            let non_executor_new = build_tuple_new(&struct_name, &[], quote!(not(feature = #feature)));
            (executor_struct, non_executor_struct, Some(args_struct), executor_new, non_executor_new)
        }
    };

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
