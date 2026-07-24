use std::{any::{Any, TypeId}, collections::HashMap, ops::Deref, sync::Arc};

use parking_lot::Mutex;

pub trait Factory<Di> : Sized {
    fn create(container: &Di) -> Self;
}

pub trait Resolve<T>: Sized{
    fn resolve(&self) -> T;
}

pub trait Get<T> {
    fn get(&self) -> T;
}

#[derive(Default, Clone)]
pub struct Container(Arc<Mutex<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>>);

impl<T> Resolve<T> for Container 
    where 
        T: Factory<Self> + Clone + Send + Sync + 'static
{
    fn resolve(&self) -> T {
        let Some(instance) = self.get() else {
            let instance = T::create(self);
            self.register(instance.clone());
            return instance;
        };

        instance
    }
}

impl<T> Get<T> for Container where T: Clone + Send + Sync + 'static {
    fn get(&self) -> T {
        let type_id = TypeId::of::<Arc<T>>();
        self.0
            .lock()
            .get(&type_id)
            .and_then(|any_ptr| any_ptr.clone().downcast::<Arc<T>>().ok())
            .map(|boxed_arc| (*boxed_arc).deref().deref().clone())
            .unwrap()
    }
}

impl Container {
    pub fn register<T: Send + Sync + 'static>(&self, instance: T) {
        let type_id = TypeId::of::<T>();
        self.0.lock().insert(type_id, Arc::new(instance));
    }
}