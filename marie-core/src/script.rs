#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Javascript {
    entrypoint: String,
    source: String
}