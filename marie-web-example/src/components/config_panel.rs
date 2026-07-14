//! Panneau de configuration : CRUD des 4 catalogues (modèles/tools/experts/
//! graphes d'états). Le graphe d'états est composé visuellement (voir
//! `marie_leptos::StateGraphEditor`) ; les autres sont de simples
//! formulaires — aucun n'a besoin d'un éditeur dédié.
//!
//! Compilé sous les deux features (voir la doc de `chat_view`) — seul
//! [`StateGraphEditorSlot`] a deux corps distincts par feature : l'éditeur
//! visuel dessine sur un `<canvas>` via `web_sys::CanvasRenderingContext2d`
//! dans un `Effect` (voir `marie_leptos::state_graph_editor`), qui ne
//! s'exécute jamais côté serveur — un `<canvas>` nu sans ce script ne
//! représenterait donc jamais rien d'utile pendant le rendu SSR ; autant
//! rendre un espace réservé statique plutôt que le vrai composant à ce
//! moment-là (`leptos` 0.8 n'a pas de composant `ClientOnly` intégré, voir
//! la recherche menée avant cette migration).

use leptos::prelude::*;
use leptos::task::spawn_local;
use marie_leptos::types::{EdgeView, ExecutableView, NodeView};

use crate::api;
use crate::dto::{EdgeDto, ExecutableDto, ExpertDto, ModelDto, NodeDto, StateGraphDto, ToolDto};

fn executable_view_to_dto(view: ExecutableView) -> ExecutableDto {
    match view {
        ExecutableView::Rust { id } => ExecutableDto::Rust { id },
        ExecutableView::Python { source } => ExecutableDto::Python { source },
        ExecutableView::Rune { source } => ExecutableDto::Rune { source },
        ExecutableView::Agent { expert_id, task } => ExecutableDto::Agent { expert_id, task },
    }
}

fn set_node_action(nodes: RwSignal<Vec<NodeView>>, node_id: &str, action: Option<ExecutableView>) {
    nodes.update(|list| {
        if let Some(node) = list.iter_mut().find(|node| node.id == node_id) {
            node.action = action;
        }
    });
}

fn set_edge_guard(edges: RwSignal<Vec<EdgeView>>, from: &str, to: &str, guard: Option<ExecutableView>) {
    edges.update(|list| {
        if let Some(edge) = list.iter_mut().find(|edge| edge.from == from && edge.to == to) {
            edge.guard = guard;
        }
    });
}

/// Espace réservé affiché côté serveur (voir la doc de module) — remplacé
/// par le vrai `StateGraphEditor` une fois l'hydratation faite.
#[cfg(not(feature = "hydrate"))]
#[component]
fn StateGraphEditorSlot(nodes: RwSignal<Vec<NodeView>>, edges: RwSignal<Vec<EdgeView>>, #[prop(into)] entry: Signal<Option<String>>) -> impl IntoView {
    let _ = (nodes, edges, entry);
    view! { <div class="state-graph-editor-placeholder">"Chargement de l'éditeur…"</div> }
}

#[cfg(feature = "hydrate")]
#[component]
fn StateGraphEditorSlot(nodes: RwSignal<Vec<NodeView>>, edges: RwSignal<Vec<EdgeView>>, #[prop(into)] entry: Signal<Option<String>>) -> impl IntoView {
    view! { <marie_leptos::StateGraphEditor nodes=nodes edges=edges entry=entry /> }
}

/// Petite ligne réutilisable pour saisir un `Option<Executable>` (action de
/// nœud ou garde d'arête, voir `marie_core::mode::executable::Executable`) —
/// `on_change` est rappelé à chaque modification, `initial` ne sert qu'à
/// pré-remplir les champs au montage.
#[component]
fn ExecutableEditor(#[prop(into)] initial: Option<ExecutableView>, #[prop(into)] on_change: Callback<Option<ExecutableView>>) -> impl IntoView {
    let kind = RwSignal::new(
        match &initial {
            None => "none",
            Some(ExecutableView::Rust { .. }) => "rust",
            Some(ExecutableView::Python { .. }) => "python",
            Some(ExecutableView::Rune { .. }) => "rune",
            Some(ExecutableView::Agent { .. }) => "agent",
        }
        .to_string(),
    );
    let field_a = RwSignal::new(match &initial {
        Some(ExecutableView::Rust { id }) => id.clone(),
        Some(ExecutableView::Python { source } | ExecutableView::Rune { source }) => source.clone(),
        Some(ExecutableView::Agent { expert_id, .. }) => expert_id.clone(),
        _ => String::new(),
    });
    let field_b = RwSignal::new(match &initial {
        Some(ExecutableView::Agent { task, .. }) => task.clone(),
        _ => String::new(),
    });

    fn emit(kind: RwSignal<String>, field_a: RwSignal<String>, field_b: RwSignal<String>, on_change: Callback<Option<ExecutableView>>) {
        let value = match kind.get_untracked().as_str() {
            "rust" => Some(ExecutableView::Rust { id: field_a.get_untracked() }),
            "python" => Some(ExecutableView::Python { source: field_a.get_untracked() }),
            "rune" => Some(ExecutableView::Rune { source: field_a.get_untracked() }),
            "agent" => Some(ExecutableView::Agent { expert_id: field_a.get_untracked(), task: field_b.get_untracked() }),
            _ => None,
        };
        on_change.run(value);
    }

    view! {
        <span class="executable-editor">
            <select prop:value=move || kind.get() on:change=move |ev| {
                kind.set(event_target_value(&ev));
                emit(kind, field_a, field_b, on_change);
            }>
                <option value="none">"(aucune)"</option>
                <option value="rust">"rust"</option>
                <option value="python">"python"</option>
                <option value="rune">"rune"</option>
                <option value="agent">"agent"</option>
            </select>
            <Show when=move || kind.get() != "none">
                <input placeholder=move || if kind.get() == "agent" { "expert_id" } else { "id / source" }
                    prop:value=move || field_a.get()
                    on:input=move |ev| { field_a.set(event_target_value(&ev)); emit(kind, field_a, field_b, on_change); } />
            </Show>
            <Show when=move || kind.get() == "agent">
                <input placeholder="task" prop:value=move || field_b.get()
                    on:input=move |ev| { field_b.set(event_target_value(&ev)); emit(kind, field_a, field_b, on_change); } />
            </Show>
        </span>
    }
}

#[component]
fn ModelsSection() -> impl IntoView {
    let models = RwSignal::new(Vec::<ModelDto>::new());
    let status = RwSignal::new(String::new());
    let id = RwSignal::new(String::new());
    let base_url = RwSignal::new(String::new());
    let client_id = RwSignal::new(String::new());
    let api_key = RwSignal::new(String::new());
    let model_name = RwSignal::new(String::new());
    let system_prompt = RwSignal::new(String::new());

    let refresh = move || {
        spawn_local(async move {
            match api::list_models().await {
                Ok(list) => models.set(list),
                Err(error) => status.set(format!("échec de chargement des modèles : {error}")),
            }
        });
    };
    Effect::new(move |_| refresh());

    let on_create = move |_| {
        let dto = ModelDto {
            id: id.get_untracked(),
            base_url: base_url.get_untracked(),
            client_id: client_id.get_untracked(),
            api_key: api_key.get_untracked(),
            model: model_name.get_untracked(),
            system_prompt: Some(system_prompt.get_untracked()).filter(|value| !value.is_empty()),
        };
        spawn_local(async move {
            match api::put_model(dto).await {
                Ok(()) => refresh(),
                Err(error) => status.set(format!("échec de création du modèle : {error}")),
            }
        });
    };

    view! {
        <section class="panel">
            <h2>"Modèles"</h2>
            <ul>
                <For each=move || models.get() key=|model| model.id.clone() let:model>
                    <li>
                        <strong>{model.id.clone()}</strong>" — "{model.model.clone()}" @ "{model.base_url.clone()}
                        <button class="link" on:click=move |_| {
                            let id = model.id.clone();
                            spawn_local(async move {
                                if let Err(error) = api::delete_model(id).await { status.set(format!("échec de suppression : {error}")); }
                                refresh();
                            });
                        }>"Supprimer"</button>
                    </li>
                </For>
            </ul>
            <p class="status">{move || status.get()}</p>
            <div class="form-grid">
                <input placeholder="id" prop:value=move || id.get() on:input=move |ev| id.set(event_target_value(&ev)) />
                <input placeholder="base_url" prop:value=move || base_url.get() on:input=move |ev| base_url.set(event_target_value(&ev)) />
                <input placeholder="client_id" prop:value=move || client_id.get() on:input=move |ev| client_id.set(event_target_value(&ev)) />
                <input placeholder="api_key" type="password" prop:value=move || api_key.get() on:input=move |ev| api_key.set(event_target_value(&ev)) />
                <input placeholder="model" prop:value=move || model_name.get() on:input=move |ev| model_name.set(event_target_value(&ev)) />
                <textarea rows="3" placeholder="system_prompt (optionnel)" prop:value=move || system_prompt.get()
                    on:input=move |ev| system_prompt.set(event_target_value(&ev)) />
                <button on:click=on_create>"Créer"</button>
            </div>
        </section>
    }
}

#[component]
fn ToolsSection() -> impl IntoView {
    let tools = RwSignal::new(Vec::<ToolDto>::new());
    let status = RwSignal::new(String::new());
    let id = RwSignal::new(String::new());
    let name = RwSignal::new(String::new());
    let description = RwSignal::new(String::new());
    let parameters_schema = RwSignal::new("{}".to_string());
    let scope = RwSignal::new("global".to_string());

    let refresh = move || {
        spawn_local(async move {
            match api::list_tools().await {
                Ok(list) => tools.set(list),
                Err(error) => status.set(format!("échec de chargement des tools : {error}")),
            }
        });
    };
    Effect::new(move |_| refresh());

    let on_create = move |_| {
        let schema = match serde_json::from_str(&parameters_schema.get_untracked()) {
            Ok(value) => value,
            Err(error) => {
                status.set(format!("parameters_schema invalide : {error}"));
                return;
            }
        };
        let dto = ToolDto {
            id: id.get_untracked(),
            name: name.get_untracked(),
            description: description.get_untracked(),
            parameters_schema: schema,
            scope: scope.get_untracked(),
        };
        spawn_local(async move {
            match api::put_tool(dto).await {
                Ok(()) => refresh(),
                Err(error) => status.set(format!("échec de création du tool : {error}")),
            }
        });
    };

    view! {
        <section class="panel">
            <h2>"Tools"</h2>
            <ul>
                <For each=move || tools.get() key=|tool| tool.id.clone() let:tool>
                    <li>
                        <strong>{tool.id.clone()}</strong>" — "{tool.name.clone()}" ("{tool.scope.clone()}")"
                        <button class="link" on:click=move |_| {
                            let id = tool.id.clone();
                            spawn_local(async move {
                                if let Err(error) = api::delete_tool(id).await { status.set(format!("échec de suppression : {error}")); }
                                refresh();
                            });
                        }>"Supprimer"</button>
                    </li>
                </For>
            </ul>
            <p class="status">{move || status.get()}</p>
            <div class="form-grid">
                <input placeholder="id" prop:value=move || id.get() on:input=move |ev| id.set(event_target_value(&ev)) />
                <input placeholder="name" prop:value=move || name.get() on:input=move |ev| name.set(event_target_value(&ev)) />
                <input placeholder="description" prop:value=move || description.get() on:input=move |ev| description.set(event_target_value(&ev)) />
                <select prop:value=move || scope.get() on:change=move |ev| scope.set(event_target_value(&ev))>
                    <option value="global">"global"</option>
                    <option value="session">"session"</option>
                </select>
                <textarea rows="3" placeholder="parameters_schema (JSON)" prop:value=move || parameters_schema.get()
                    on:input=move |ev| parameters_schema.set(event_target_value(&ev)) />
                <button on:click=on_create>"Créer"</button>
            </div>
        </section>
    }
}

#[component]
fn ExpertsSection() -> impl IntoView {
    let experts = RwSignal::new(Vec::<ExpertDto>::new());
    let models = RwSignal::new(Vec::<ModelDto>::new());
    let status = RwSignal::new(String::new());
    let id = RwSignal::new(String::new());
    let prompt = RwSignal::new(String::new());
    let model_id = RwSignal::new(String::new());
    let allowed_tools = RwSignal::new(String::new());

    let refresh = move || {
        spawn_local(async move {
            match api::list_experts().await {
                Ok(list) => experts.set(list),
                Err(error) => status.set(format!("échec de chargement des experts : {error}")),
            }
        });
    };
    Effect::new(move |_| refresh());
    Effect::new(move |_| {
        spawn_local(async move {
            match api::list_models().await {
                Ok(list) => models.set(list),
                Err(error) => status.set(format!("échec de chargement des modèles : {error}")),
            }
        });
    });

    let on_create = move |_| {
        let dto = ExpertDto {
            id: id.get_untracked(),
            prompt: prompt.get_untracked(),
            model_id: model_id.get_untracked(),
            allowed_tools: allowed_tools.get_untracked().split(',').map(str::trim).filter(|tool| !tool.is_empty()).map(str::to_string).collect(),
        };
        spawn_local(async move {
            match api::put_expert(dto).await {
                Ok(()) => refresh(),
                Err(error) => status.set(format!("échec de création de l'expert : {error}")),
            }
        });
    };

    view! {
        <section class="panel">
            <h2>"Experts"</h2>
            <ul>
                <For each=move || experts.get() key=|expert| expert.id.clone() let:expert>
                    <li>
                        <strong>{expert.id.clone()}</strong>" — modèle "{expert.model_id.clone()}
                        <button class="link" on:click=move |_| {
                            let id = expert.id.clone();
                            spawn_local(async move {
                                if let Err(error) = api::delete_expert(id).await { status.set(format!("échec de suppression : {error}")); }
                                refresh();
                            });
                        }>"Supprimer"</button>
                    </li>
                </For>
            </ul>
            <p class="status">{move || status.get()}</p>
            <div class="form-grid">
                <input placeholder="id" prop:value=move || id.get() on:input=move |ev| id.set(event_target_value(&ev)) />
                <select prop:value=move || model_id.get() on:change=move |ev| model_id.set(event_target_value(&ev))>
                    <option value="">"(choisir un modèle)"</option>
                    <For each=move || models.get() key=|model| model.id.clone() let:model>
                        <option value=model.id.clone()>{model.id.clone()}" — "{model.model.clone()}</option>
                    </For>
                </select>
                <textarea rows="3" placeholder="prompt" prop:value=move || prompt.get() on:input=move |ev| prompt.set(event_target_value(&ev)) />
                <input placeholder="allowed_tools (séparés par des virgules)" prop:value=move || allowed_tools.get()
                    on:input=move |ev| allowed_tools.set(event_target_value(&ev)) />
                <button on:click=on_create>"Créer"</button>
            </div>
        </section>
    }
}

#[component]
fn StateGraphsSection() -> impl IntoView {
    let graphs = RwSignal::new(Vec::<StateGraphDto>::new());
    let status = RwSignal::new(String::new());
    let id = RwSignal::new(String::new());
    let entry = RwSignal::new(String::new());
    let nodes = RwSignal::new(Vec::<NodeView>::new());
    let edges = RwSignal::new(Vec::<EdgeView>::new());

    let refresh = move || {
        spawn_local(async move {
            match api::list_state_graphs().await {
                Ok(list) => graphs.set(list),
                Err(error) => status.set(format!("échec de chargement des graphes : {error}")),
            }
        });
    };
    Effect::new(move |_| refresh());

    let on_create = move |_| {
        let node_dtos: Vec<NodeDto> =
            nodes.get_untracked().into_iter().map(|node| NodeDto { id: node.id, action: node.action.map(executable_view_to_dto) }).collect();
        let edge_dtos: Vec<EdgeDto> = edges
            .get_untracked()
            .into_iter()
            .map(|edge| EdgeDto { from: edge.from, to: edge.to, guard: edge.guard.map(executable_view_to_dto) })
            .collect();
        let dto = StateGraphDto { id: id.get_untracked(), entry: entry.get_untracked(), nodes: node_dtos, edges: edge_dtos };
        spawn_local(async move {
            match api::put_state_graph(dto).await {
                Ok(()) => refresh(),
                Err(error) => status.set(format!("échec de création du graphe : {error}")),
            }
        });
    };

    view! {
        <section class="panel">
            <h2>"Graphes d'états"</h2>
            <ul>
                <For each=move || graphs.get() key=|graph| graph.id.clone() let:graph>
                    <li>
                        <strong>{graph.id.clone()}</strong>" — entrée "{graph.entry.clone()}" ("{graph.nodes.len()}" nœuds, "{graph.edges.len()}" arêtes)"
                        <button class="link" on:click=move |_| {
                            let id = graph.id.clone();
                            spawn_local(async move {
                                if let Err(error) = api::delete_state_graph(id).await { status.set(format!("échec de suppression : {error}")); }
                                refresh();
                            });
                        }>"Supprimer"</button>
                    </li>
                </For>
            </ul>
            <p class="status">{move || status.get()}</p>

            <h3>"Nouveau graphe"</h3>
            <p class="hint">"Double-clic : créer un nœud — glisser depuis la poignée : relier — clic-droit : supprimer."</p>
            <StateGraphEditorSlot nodes=nodes edges=edges entry=Signal::derive(move || Some(entry.get())) />

            <h4>"Nœuds"</h4>
            <ul>
                <For each=move || nodes.get() key=|node| node.id.clone() let:node>
                    {
                        let node_id = node.id.clone();
                        let initial = node.action.clone();
                        view! {
                            <li>
                                <strong>{node.id.clone()}</strong>
                                <ExecutableEditor initial=initial on_change=Callback::new(move |value| set_node_action(nodes, &node_id, value)) />
                            </li>
                        }
                    }
                </For>
            </ul>

            <h4>"Arêtes"</h4>
            <ul>
                <For each=move || edges.get() key=|edge| format!("{}->{}", edge.from, edge.to) let:edge>
                    {
                        let (from, to) = (edge.from.clone(), edge.to.clone());
                        let initial = edge.guard.clone();
                        view! {
                            <li>
                                <strong>{edge.from.clone()}" → "{edge.to.clone()}</strong>
                                <ExecutableEditor initial=initial on_change=Callback::new(move |value| set_edge_guard(edges, &from, &to, value)) />
                            </li>
                        }
                    }
                </For>
            </ul>

            <div class="form-grid">
                <input placeholder="id du graphe" prop:value=move || id.get() on:input=move |ev| id.set(event_target_value(&ev)) />
                <select prop:value=move || entry.get() on:change=move |ev| entry.set(event_target_value(&ev))>
                    <option value="">"(choisir le nœud d'entrée)"</option>
                    <For each=move || nodes.get() key=|node| node.id.clone() let:node>
                        <option value=node.id.clone()>{node.id.clone()}</option>
                    </For>
                </select>
                <button on:click=on_create>"Créer"</button>
            </div>
        </section>
    }
}

#[component]
pub fn ConfigPanel() -> impl IntoView {
    view! {
        <div class="config-panel">
            <ModelsSection />
            <ToolsSection />
            <ExpertsSection />
            <StateGraphsSection />
        </div>
    }
}
