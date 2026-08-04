use std::{any::{Any, TypeId}, collections::HashMap, sync::Arc};

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
    fn try_get(&self) -> Option<T>;
    
    fn get(&self) -> T {
        self.try_get().unwrap()
    }
}

#[derive(Default, Clone)]
pub struct Container(Arc<Mutex<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>>);

impl<T, Args> Resolve<T, Args> for Container 
    where 
        T: Constructible<Self, Args> + Clone + Send + Sync + 'static
{
    fn resolve(&self, args: Args) -> T {
        let Some(instance) = self.try_get() else {
            let instance = T::construct(self, args);
            self.register(instance.clone());
            return instance;
        };

        instance
    }
}

impl<T> Get<T> for Container where T: Clone + Send + Sync + 'static {
    fn try_get(&self) -> Option<T> {
        // La clé et la cible du downcast doivent correspondre exactement à
        // ce que `Container::register` stocke : `TypeId::of::<T>()` comme
        // clé, un `Arc<dyn Any>` dont le type érasé est `T` (pas `Arc<T>`)
        // comme valeur — `Arc::new(instance)` s'y coerce en `Arc<dyn Any>`
        // en érasant `T`, pas `Arc<T>`.
        let type_id = TypeId::of::<T>();
        self.0
            .lock()
            .get(&type_id)
            .and_then(|any_ptr| any_ptr.clone().downcast::<T>().ok())
            .map(|boxed| (*boxed).clone())
    }
}

impl Container {
    pub fn register<T: Send + Sync + 'static>(&self, instance: T) {
        let type_id = TypeId::of::<T>();
        self.0.lock().insert(type_id, Arc::new(instance));
    }
}