//! Coquille HTML (`shell`, rendue par `leptos_axum` pour chaque requête, en
//! remplacement de l'`index.html` statique de l'ancien crate CSR) et racine
//! de l'app (`App`) — compilée sans distinction sous les deux features
//! `ssr`/`hydrate` (voir la doc de `crate::lib` sur ce découpage).

use leptos::prelude::*;

use crate::components::{ChatView, ConfigPanel};

pub fn shell(options: LeptosOptions) -> impl IntoView {
    view! {
        <!DOCTYPE html>
        <html lang="fr">
            <head>
                <meta charset="utf-8"/>
                <meta name="viewport" content="width=device-width, initial-scale=1"/>
                <title>"Marie — exemple axum + leptos"</title>
                <AutoReload options=options.clone() />
                <HydrationScripts options/>
                <link rel="stylesheet" href="/style.css"/>
            </head>
            <body>
                <App/>
            </body>
        </html>
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Tab {
    Chat,
    Config,
}

#[component]
pub fn App() -> impl IntoView {
    let tab = RwSignal::new(Tab::Chat);

    view! {
        <div class="app">
            <nav class="tabs">
                <button class:active=move || tab.get() == Tab::Chat on:click=move |_| tab.set(Tab::Chat)>"Chat"</button>
                <button class:active=move || tab.get() == Tab::Config on:click=move |_| tab.set(Tab::Config)>"Configuration"</button>
            </nav>
            <main>
                {move || match tab.get() {
                    Tab::Chat => view!{<ChatView/>}.into_any(),
                    Tab::Config => view!{<ConfigPanel/>}.into_any(),
                }}
            </main>
        </div>
    }
}
