//! Formulaire humain-dans-la-boucle (voir
//! `marie_core::hitl::HumanInputRequest`) — contrairement à `session_panel`
//! (headless, rendu délégué à l'appelant), ce composant rend directement le
//! formulaire : les types de question qu'il porte (texte, choix, fichier)
//! ont une structure suffisamment fixe pour qu'un rendu tout fait ait de la
//! valeur, sur le même principe que [`crate::state_graph_editor::StateGraphEditor`].
//!
//! `answers` reste possédé par l'appelant (comme `nodes`/`edges` de
//! `StateGraphEditor`) : ce composant les lit/modifie mais ne les initialise
//! ni ne les soumet lui-même — c'est à l'appelant de les envoyer (voir
//! `on_submit`) et de gérer le contenu d'un fichier téléversé (voir
//! `on_file_selected` : ce composant ne fait que remonter le
//! [`web_sys::File`] choisi, jamais son contenu — écrire ce contenu quelque
//! part est une opération réseau, hors du périmètre de ce crate, voir sa
//! doc de module).

use std::collections::HashMap;

use leptos::callback::UnsyncCallback;
use leptos::prelude::*;
use wasm_bindgen::JsCast as _;

use crate::types::{AnswerView, HitlRequestView, QuestionKindView, QuestionView};

fn single_value(answers: RwSignal<HashMap<String, AnswerView>>, key: &str) -> String {
    match answers.get().get(key) {
        Some(AnswerView::Single(value)) => value.clone(),
        _ => String::new(),
    }
}

#[component]
fn HitlQuestion(
    question: QuestionView,
    #[prop(into)] answers: RwSignal<HashMap<String, AnswerView>>,
    #[prop(into)] on_file_selected: UnsyncCallback<(String, web_sys::File)>,
) -> impl IntoView {
    let label = question.label.clone();
    let key = question.key;

    let body = match question.kind {
        QuestionKindView::ShortText => {
            let (key_write, key_read) = (key.clone(), key.clone());
            view! {
                <input type="text" prop:value=move || single_value(answers, &key_read)
                    on:input=move |ev| { answers.update(|map| { map.insert(key_write.clone(), AnswerView::Single(event_target_value(&ev))); }); } />
            }.into_any()
        }
        QuestionKindView::LongText => {
            let (key_write, key_read) = (key.clone(), key.clone());
            view! {
                <textarea prop:value=move || single_value(answers, &key_read)
                    on:input=move |ev| { answers.update(|map| { map.insert(key_write.clone(), AnswerView::Single(event_target_value(&ev))); }); } />
            }.into_any()
        }
        QuestionKindView::Select { options } => {
            let (key_write, key_read) = (key.clone(), key.clone());
            view! {
                <select prop:value=move || single_value(answers, &key_read)
                    on:change=move |ev| { answers.update(|map| { map.insert(key_write.clone(), AnswerView::Single(event_target_value(&ev))); }); }>
                    <option value="">"—"</option>
                    <For each=move || options.clone() key=|option| option.clone() let:option>
                        {
                            let (value, label) = (option.clone(), option);
                            view! { <option value=value>{label}</option> }
                        }
                    </For>
                </select>
            }.into_any()
        }
        QuestionKindView::Radio { options } => {
            let name = key.clone();
            view! {
                <div class="hitl-radio-group">
                    <For each=move || options.clone() key=|option| option.clone() let:option>
                        {
                            let (key_write, key_read) = (key.clone(), key.clone());
                            let (option_write, option_read) = (option.clone(), option.clone());
                            let name = name.clone();
                            view! {
                                <label class="hitl-radio">
                                    <input type="radio" name=name.clone()
                                        prop:checked=move || matches!(answers.get().get(&key_read), Some(AnswerView::Single(value)) if value == &option_read)
                                        on:change=move |_| { answers.update(|map| { map.insert(key_write.clone(), AnswerView::Single(option_write.clone())); }); } />
                                    {option}
                                </label>
                            }
                        }
                    </For>
                </div>
            }.into_any()
        }
        QuestionKindView::Checkboxes { options } => {
            view! {
                <div class="hitl-checkbox-group">
                    <For each=move || options.clone() key=|option| option.clone() let:option>
                        {
                            let (key_write, key_read) = (key.clone(), key.clone());
                            let (option_write, option_read) = (option.clone(), option.clone());
                            view! {
                                <label class="hitl-checkbox">
                                    <input type="checkbox"
                                        prop:checked=move || matches!(answers.get().get(&key_read), Some(AnswerView::Multiple(values)) if values.contains(&option_read))
                                        on:change=move |ev| {
                                            let checked = event_target_checked(&ev);
                                            answers.update(|map| {
                                                let entry = map.entry(key_write.clone()).or_insert_with(|| AnswerView::Multiple(Vec::new()));
                                                if let AnswerView::Multiple(values) = entry {
                                                    if checked {
                                                        if !values.contains(&option_write) {
                                                            values.push(option_write.clone());
                                                        }
                                                    } else {
                                                        values.retain(|value| value != &option_write);
                                                    }
                                                }
                                            });
                                        } />
                                    {option}
                                </label>
                            }
                        }
                    </For>
                </div>
            }.into_any()
        }
        QuestionKindView::FileUpload { accept } => {
            let (key_write, key_read) = (key.clone(), key.clone());
            let accept_attr = accept.join(",");
            view! {
                <div class="hitl-file">
                    <input type="file" accept=accept_attr
                        on:change=move |ev| {
                            let Some(target) = ev.target() else { return };
                            let Ok(input) = target.dyn_into::<web_sys::HtmlInputElement>() else { return };
                            let Some(files) = input.files() else { return };
                            let Some(file) = files.get(0) else { return };
                            on_file_selected.run((key_write.clone(), file));
                        } />
                    <span class="hitl-file-name">
                        {move || {
                            let value = single_value(answers, &key_read);
                            if value.is_empty() { "(aucun fichier)".to_string() } else { value }
                        }}
                    </span>
                </div>
            }.into_any()
        }
    };

    view! {
        <div class="hitl-question">
            <label class="hitl-question-label">{label}</label>
            {body}
        </div>
    }
}

/// Rend le formulaire `request`, un champ par [`QuestionView`] selon son
/// [`QuestionKindView`]. `answers` doit être initialisé par l'appelant avant
/// montage (une `HashMap` vide convient : chaque question part alors sans
/// réponse) et lu à nouveau à la soumission (voir `on_submit`, qui ne reçoit
/// aucun argument — l'appelant lit `answers` lui-même, il le possède déjà).
#[component]
pub fn HitlForm(
    #[prop(into)] request: HitlRequestView,
    #[prop(into)] answers: RwSignal<HashMap<String, AnswerView>>,
    #[prop(into)] on_file_selected: UnsyncCallback<(String, web_sys::File)>,
    #[prop(into)] on_submit: UnsyncCallback<()>,
) -> impl IntoView {
    let questions = request.questions;

    view! {
        <form class="hitl-form" on:submit=move |ev| {
            ev.prevent_default();
            on_submit.run(());
        }>
            <For each=move || questions.clone() key=|question| question.key.clone() let:question>
                <HitlQuestion question=question answers=answers on_file_selected=on_file_selected />
            </For>
            <button type="submit">"Envoyer"</button>
        </form>
    }
}
