use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CatalogRef {
    pub kind: String,
    pub id: String,
    pub version: u64,
}

pub trait Catalogable: Serialize + Deserialize + Clone + 'static {
    const KIND: &str;

    fn id(&self) -> &str;
}

#[async_trait]
pub trait Catalog {
    async fn publish<C>(&self, item: &C) -> CatalogRef where C: Catalogable;
    async fn get<C>(&self, r: &CatalogRef) -> Result<C, CatalogError> where C: Catalogable;
    async fn latest<C>(&self, id: &str) -> Result<CatalogRef, CatalogError> where C: Catalogable;
    async fn list<C>(&self) -> Vec<CatalogRef>;
    async fn deprecate(&self, r: &CatalogRef) -> Result<(), CatalogError>;
}

