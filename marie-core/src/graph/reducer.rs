use serde_json::Value;

pub enum Reducer {
    Javascript {
        code: String
    },
}