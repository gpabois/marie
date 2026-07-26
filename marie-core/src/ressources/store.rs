pub trait ResourceStore {
    async fn get(&self, path: &ResourcePath) -> Result<VersionedDoc, StoreError>;
    async fn patch(&self, path: &ResourcePath, ops: json_patch::Patch, expected_version: u64) -> Result<VersionedDoc, StoreError>;
    async fn list_children(&self, prefix: &ResourcePath) -> Vec<ResourcePath>;
}