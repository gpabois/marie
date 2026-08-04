use std::ops::{Deref, DerefMut};

use async_trait::async_trait;

pub struct UnitOfWork<E, S: UnitOfWorkExecutor<E>> {
    pub executor: S,
    pub entity: E,
    pub dirty: bool,
    pub create: bool,
    pub deleted: bool,
}

impl<E, S: UnitOfWorkExecutor<E>> UnitOfWork<E, S> {
    pub fn new(entity: E, executor: S) -> Self {
        Self {
            executor,
            entity,
            dirty: false,
            create: false,
            deleted: false
        }
    }
}

impl<E, S: UnitOfWorkExecutor<E>> Deref for UnitOfWork<E, S> {
    type Target = E;

    fn deref(&self) -> &Self::Target {
        if self.deleted {
            panic!("entity has been deleted");
        }
        &self.entity
    }
}

impl<E, S: UnitOfWorkExecutor<E>> DerefMut for UnitOfWork<E, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        if self.deleted {
            panic!("entity has been deleted");
        }
        self.dirty = true;
        &mut self.entity
    }
}

impl<E, S: UnitOfWorkExecutor<E>> UnitOfWork<E, S> {
    pub async fn flush(&mut self) -> crate::Result<()> {
        if self.dirty {
            match (self.create, self.deleted) {
                (true, true) => {}, // do nothing
                (true, false) => {
                    self.executor.insert(&self.entity).await?
                }, 
                (false, true) => {
                    self.executor.delete(&self.entity).await?
                },
                (false, false) => {
                    self.executor.replace(&self.entity).await?
                }
            }

            self.dirty = false;
        }

        Ok(())
    }
}

#[async_trait]
pub trait UnitOfWorkExecutor<E> {
    async fn insert(&self, entity: &E) -> crate::Result<()>;
    async fn replace(&self, entity: &E) -> crate::Result<()>;
    async fn delete(&self, entity: &E) -> crate::Result<()>;
}