use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use marie_macros::core_spec;

use crate::{agent::Context, session::channel::{ChannelName, ChannelSpec, Reducer}};

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct CommonSpec {
    pub budget: Budget,
    pub channels: Vec<ChannelSpec>,
    pub inherited_channels: Vec<ChannelName>,
    pub exported_channels: Vec<ChannelName>,
    pub default_values: HashMap<ChannelName, Value>,
    /// Valeurs fixées pour un canal, indépendamment de ce que fournirait
    /// l'héritage (voir [`Self::inherited_channels`]) — sur le même principe
    /// que [`Self::default_values`], mais prioritaire dessus. Construit par
    /// [`core_spec!`]/`spec!` via le champ `overrides` de chaque canal.
    pub overrides_channels: HashMap<ChannelName, Value>,
    /// Canaux que ce frame accepte de recevoir de ses enfants à leur
    /// terminaison (voir `SessionHandler::drain_pending_accumulators`) —
    /// symétrique à [`Self::inherited_channels`] (ancêtre → frame) mais pour
    /// la direction montante (enfant → frame). Un canal
    /// [`exported_channels`](Self::exported_channels) d'un enfant qui
    /// n'apparaît pas ici est silencieusement ignoré. Construit par
    /// [`core_spec!`]/`spec!` via le champ `imported` de chaque canal.
    pub imported_channels: Vec<ChannelName>
}

impl CommonSpec {
    pub fn overrides(&mut self, channel_name: impl Into<ChannelName>, value: impl Serialize) {
        self.overrides_channels.insert(channel_name.into(), serde_json::to_value(value).unwrap());
    }
}

impl CommonSpec {
    pub fn shell() -> CommonSpec {
        core_spec! {
            channels = {
                "tool_result": {
                    reducer: Reducer::LastWriteWins,
                    imported: true
                },
                "expert_answer": {
                    reducer: Reducer::LastWriteWins,
                    imported: true           
                },
                "hitl_answer": {
                    reducer: Reducer::LastWriteWins,
                    imported: true
                }
            }
        }
    }
    pub fn graph() -> CommonSpec {
        core_spec! {
            channels = {
                "graph_result": {
                    reducer: Reducer::LastWriteWins,
                    exported: true             
                }
            }
        }
    }
    pub fn tool() -> CommonSpec {
        core_spec! {
            channels = {
                "tool_result": {
                    reducer: Reducer::LastWriteWins,
                    exported: true
                }
            }
        }
    }
    pub fn tool_aggregator() -> CommonSpec {
        core_spec! {
            channels = {
                "tool_result": {
                    reducer: Reducer::Append,
                    imported: true,
                    exported: true
                }
            }
        }
    }
    /// Fixe temporaire en attendant de 
    /// modifier Expert en ExpertSpec
    pub fn expert() -> CommonSpec {
        core_spec! {
            channels = {
                "expert_task": {
                    reducer: Reducer::LastWriteWins,
                    inherited: true
                },
                "expert_answer": {
                    reducer: Reducer::LastWriteWins,
                    exported: true,
                    imported: true
                },
                "expert_context": {
                    reducer: Reducer::LastWriteWins,
                    default: Context::default()
                },
                "tool_result": {
                    reducer: Reducer::LastWriteWins,
                    default: Vec::<Value>::default(),
                    inherited: true,
                    imported: true
                },
            }
        }
    }
    pub fn expert_aggregator() -> CommonSpec {
        core_spec! {
            channels = {
                "expert_task": {
                    reducer: Reducer::LastWriteWins,
                    inherited: true
                },
                "expert_answer": {
                    reducer: Reducer::Append,
                    inherited: true,
                    imported: true,
                    exported: true
                }
            }
        }     
    }
    pub fn hitl() -> CommonSpec {
        CommonSpec {
            channels: vec![
                ChannelSpec::new("hitl_answer", Reducer::LastWriteWins)
            ],
            inherited_channels: vec![],
            exported_channels: vec!["hitl_answer".into()],
            ..Default::default()
        }
    }

    /// Fusionne deux specs : `b` est appliquée par-dessus `a`. Pour les
    /// champs à clé (`channels`, `default_values`, `overrides_channels`,
    /// `budget`), une entrée de `b` écrase l'entrée correspondante de `a`
    /// quand les deux la définissent ; sinon les deux contribuent. Pour les
    /// listes de noms de canaux (`inherited_channels`, `exported_channels`,
    /// `imported_channels`), qui ne portent pas de valeur propre, la fusion
    /// est une simple union dédupliquée.
    pub fn merge(a: CommonSpec, b: CommonSpec) -> CommonSpec {
        let mut channels = a.channels;
        for channel in b.channels {
            match channels.iter_mut().find(|c| c.name() == channel.name()) {
                Some(existing) => *existing = channel,
                None => channels.push(channel),
            }
        }

        let mut default_values = a.default_values;
        default_values.extend(b.default_values);

        let mut overrides_channels = a.overrides_channels;
        overrides_channels.extend(b.overrides_channels);

        CommonSpec {
            budget: Budget {
                max_step: b.budget.max_step.or(a.budget.max_step),
                max_tokens: b.budget.max_tokens.or(a.budget.max_tokens),
            },
            channels,
            inherited_channels: union(a.inherited_channels, b.inherited_channels),
            exported_channels: union(a.exported_channels, b.exported_channels),
            default_values,
            overrides_channels,
            imported_channels: union(a.imported_channels, b.imported_channels),
        }
    }
}

fn union(mut a: Vec<ChannelName>, b: Vec<ChannelName>) -> Vec<ChannelName> {
    for name in b {
        if !a.contains(&name) {
            a.push(name);
        }
    }
    a
}


#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Budget {
    pub max_step: Option<u32>,
    pub max_tokens: Option<u32>
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_spec_builds_common_spec() {
        let spec = core_spec! {
            budget = { max_step: 32, max_tokens: 1024 },
            channels = {
                "task": { reducer: Reducer::LastWriteWins, inherited: true },
                "answer": { reducer: Reducer::LastWriteWins, exported: true, default: 0, overrides: 32 },
                "result": { reducer: Reducer::Append, imported: true },
            }
        };

        assert_eq!(spec.budget.max_step, Some(32));
        assert_eq!(spec.budget.max_tokens, Some(1024));
        assert_eq!(spec.channels.len(), 3);
        assert_eq!(spec.inherited_channels, vec![ChannelName::from("task")]);
        assert_eq!(spec.exported_channels, vec![ChannelName::from("answer")]);
        assert_eq!(spec.imported_channels, vec![ChannelName::from("result")]);
        assert_eq!(spec.default_values.get(&ChannelName::from("answer")), Some(&serde_json::json!(0)));
        assert_eq!(spec.overrides_channels.get(&ChannelName::from("answer")), Some(&serde_json::json!(32)));
    }

    #[test]
    fn core_spec_defaults_are_empty() {
        let spec = core_spec! {};

        assert!(spec.budget.max_step.is_none());
        assert!(spec.budget.max_tokens.is_none());
        assert!(spec.channels.is_empty());
        assert!(spec.overrides_channels.is_empty());
        assert!(spec.imported_channels.is_empty());
    }

    #[test]
    fn merge_overwrites_intersecting_entries_with_b() {
        let a = core_spec! {
            budget = { max_step: 32 },
            channels = {
                "task": { reducer: Reducer::LastWriteWins, inherited: true, default: "a" },
                "shared": { reducer: Reducer::LastWriteWins, default: "a", overrides: "a" },
            }
        };
        let b = core_spec! {
            budget = { max_tokens: 1024 },
            channels = {
                "shared": { reducer: Reducer::Append, exported: true, default: "b", overrides: "b" },
                "result": { reducer: Reducer::Append, imported: true },
            }
        };

        let merged = CommonSpec::merge(a, b);

        assert_eq!(merged.budget.max_step, Some(32));
        assert_eq!(merged.budget.max_tokens, Some(1024));

        // "shared" est présent dans les deux : b l'emporte (reducer + position).
        assert_eq!(merged.channels.len(), 3);
        let shared = merged.channels.iter().find(|c| c.name() == &ChannelName::from("shared")).unwrap();
        assert_eq!(shared.reducer(), &Reducer::Append);
        assert_eq!(merged.default_values.get(&ChannelName::from("shared")), Some(&serde_json::json!("b")));
        assert_eq!(merged.overrides_channels.get(&ChannelName::from("shared")), Some(&serde_json::json!("b")));

        // "task" ne vient que de a, "result" que de b : les deux survivent.
        assert!(merged.channels.iter().any(|c| c.name() == &ChannelName::from("task")));
        assert!(merged.channels.iter().any(|c| c.name() == &ChannelName::from("result")));

        assert_eq!(merged.inherited_channels, vec![ChannelName::from("task")]);
        assert_eq!(merged.exported_channels, vec![ChannelName::from("shared")]);
        assert_eq!(merged.imported_channels, vec![ChannelName::from("result")]);
    }

    #[test]
    fn aggregators_import_what_their_children_export() {
        assert_eq!(CommonSpec::tool_aggregator().imported_channels, vec![ChannelName::from("tool_result")]);
        assert!(CommonSpec::expert().imported_channels.contains(&ChannelName::from("tool_result")));
        assert_eq!(CommonSpec::expert_aggregator().imported_channels, vec![ChannelName::from("expert_answer")]);
    }
}