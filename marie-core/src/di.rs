use std::{any::{Any, TypeId}, collections::HashMap, ops::Deref, sync::Arc};

use parking_lot::Mutex;

pub struct Factory<T, Args=()>(Arc<dyn (Fn(Args) -> T) + Send + Sync + 'static>);

impl<T, Args> Clone for Factory<T, Args> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T, Args> Factory<T, Args> {
    pub fn new<F>(factory: F) -> Self 
        where F: Fn(Args) -> T + Send + Sync + 'static
    {
        Self(Arc::new(factory))
    }

    pub fn create(&self, args: Args) -> T {
        (self.0)(args)
    }
}

pub trait Constructible<Di, Args=()> : Sized {
    fn construct(container: &Di, args: Args) -> Self;
}

pub trait Resolve<T, Args=()>: Sized{
    fn resolve(&self, args: Args) -> T;
}

pub trait Get<T> {
    fn get(&self) -> T;
}

#[derive(Default, Clone)]
pub struct Container(Arc<Mutex<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>>);

impl<T, Args> Resolve<T, Args> for Container 
    where 
        T: Constructible<Self, Args> + Clone + Send + Sync + 'static
{
    fn resolve(&self, args: Args) -> T {
        let Some(instance) = self.get() else {
            let instance = T::construct(self, args);
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