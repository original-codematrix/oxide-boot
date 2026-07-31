use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

pub type ComponentResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub type HookFn = fn(&Context) -> futures::future::BoxFuture<'static, ComponentResult<()>>;

#[derive(Debug, thiserror::Error)]
pub enum ContextError {
    #[error("component '{component}' depends on '{dependency}', but no such component is registered")]
    MissingDependency { component: &'static str, dependency: &'static str },
    #[error("dependency cycle detected: {path}")]
    Cycle { path: String },
    #[error("component '{type_name}' is registered more than once")]
    Duplicate { type_name: &'static str },
    #[error("no component of type '{type_name}' in the context")]
    NotFound { type_name: &'static str },
    #[error("failed to construct component '{type_name}': {message}")]
    Construct { type_name: &'static str, message: String },
}

/// Shared handle to a singleton component. Cheap to clone; derefs to `T`.
pub struct Inject<T>(Arc<T>);

impl<T> Clone for Inject<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> std::ops::Deref for Inject<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

/// Static description of one component: how to identify it, what it needs,
/// how to build it. Registered via `inventory` by the `#[component]` macro.
///
/// `#[non_exhaustive]`: fields stay `pub` for reading, but construction
/// outside `oxide-core` must go through [`ComponentDescriptor::builder`].
/// This lets future fields be added as additive builder methods instead of
/// breaking every struct-literal call site.
#[derive(Debug)]
#[non_exhaustive]
pub struct ComponentDescriptor {
    pub type_name: &'static str,
    pub type_id: fn() -> TypeId,
    pub deps: fn() -> Vec<(TypeId, &'static str)>,
    pub construct: fn(&mut ContextBuilder) -> Result<(), ContextError>,
    pub post_construct: Option<HookFn>,
    pub pre_destroy: Option<HookFn>,
}

inventory::collect!(ComponentDescriptor);

fn no_deps() -> Vec<(TypeId, &'static str)> {
    Vec::new()
}

/// Builder for [`ComponentDescriptor`]. Since the descriptor is
/// `#[non_exhaustive]`, this is the only way to construct one outside of
/// `oxide-core`.
pub struct ComponentDescriptorBuilder {
    inner: ComponentDescriptor,
}

impl ComponentDescriptor {
    /// Starts building a descriptor for `type_name`. `deps` defaults to
    /// none; `post_construct`/`pre_destroy` default to `None`.
    ///
    /// `const fn`: macro codegen registers descriptors via
    /// `inventory::submit!`, which expands into a `static` initializer, so
    /// the whole builder chain must be const-evaluable.
    pub const fn builder(
        type_name: &'static str,
        type_id: fn() -> TypeId,
        construct: fn(&mut ContextBuilder) -> Result<(), ContextError>,
    ) -> ComponentDescriptorBuilder {
        ComponentDescriptorBuilder {
            inner: ComponentDescriptor {
                type_name,
                type_id,
                deps: no_deps,
                construct,
                post_construct: None,
                pre_destroy: None,
            },
        }
    }
}

impl ComponentDescriptorBuilder {
    pub const fn deps(mut self, deps: fn() -> Vec<(TypeId, &'static str)>) -> Self {
        self.inner.deps = deps;
        self
    }

    pub const fn post_construct(mut self, hook: HookFn) -> Self {
        self.inner.post_construct = Some(hook);
        self
    }

    pub const fn pre_destroy(mut self, hook: HookFn) -> Self {
        self.inner.pre_destroy = Some(hook);
        self
    }

    pub const fn build(self) -> ComponentDescriptor {
        self.inner
    }
}

#[derive(Default)]
pub struct ContextBuilder {
    map: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl ContextBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert<T: Send + Sync + 'static>(&mut self, value: T) {
        self.map.insert(TypeId::of::<T>(), Arc::new(value));
    }

    pub fn resolve<T: Send + Sync + 'static>(&self) -> Result<Inject<T>, ContextError> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|any| Arc::clone(any).downcast::<T>().ok())
            .map(Inject)
            .ok_or(ContextError::NotFound { type_name: std::any::type_name::<T>() })
    }

    pub fn contains(&self, id: TypeId) -> bool {
        self.map.contains_key(&id)
    }
}

/// Immutable, fully-wired application context.
pub struct Context {
    map: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl Context {
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<Inject<T>> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|any| Arc::clone(any).downcast::<T>().ok())
            .map(Inject)
    }
}

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("component_count", &self.map.len())
            .finish_non_exhaustive()
    }
}

/// Builds the context: topologically sorts descriptors by their dependencies,
/// constructs every component exactly once, fails fast on missing deps,
/// duplicates and cycles. Returns the construction order for lifecycle hooks.
pub fn assemble(
    descriptors: Vec<&'static ComponentDescriptor>,
    mut seed: ContextBuilder,
) -> Result<(Context, Vec<&'static ComponentDescriptor>), ContextError> {
    let mut by_id: HashMap<TypeId, &'static ComponentDescriptor> = HashMap::new();
    for d in &descriptors {
        if by_id.insert((d.type_id)(), d).is_some() {
            return Err(ContextError::Duplicate { type_name: d.type_name });
        }
    }

    let mut descriptors = descriptors;
    descriptors.sort_by_key(|d| d.type_name); // deterministic order for independents; dependency order still dominates via the DFS below

    #[derive(Clone, Copy, PartialEq)]
    enum Mark {
        Visiting,
        Done,
    }

    fn visit(
        d: &'static ComponentDescriptor,
        by_id: &HashMap<TypeId, &'static ComponentDescriptor>,
        seed: &ContextBuilder,
        marks: &mut HashMap<TypeId, Mark>,
        stack: &mut Vec<&'static str>,
        order: &mut Vec<&'static ComponentDescriptor>,
    ) -> Result<(), ContextError> {
        let id = (d.type_id)();
        match marks.get(&id) {
            Some(Mark::Done) => return Ok(()),
            Some(Mark::Visiting) => {
                stack.push(d.type_name);
                return Err(ContextError::Cycle { path: stack.join(" -> ") });
            }
            None => {}
        }
        marks.insert(id, Mark::Visiting);
        stack.push(d.type_name);
        for (dep_id, dep_name) in (d.deps)() {
            if let Some(dep) = by_id.get(&dep_id) {
                visit(dep, by_id, seed, marks, stack, order)?;
            } else if !seed.contains(dep_id) {
                return Err(ContextError::MissingDependency { component: d.type_name, dependency: dep_name });
            }
        }
        stack.pop();
        marks.insert(id, Mark::Done);
        order.push(d);
        Ok(())
    }

    let mut marks = HashMap::new();
    let mut order = Vec::new();
    for d in &descriptors {
        let mut stack = Vec::new();
        visit(d, &by_id, &seed, &mut marks, &mut stack, &mut order)?;
    }

    for d in &order {
        (d.construct)(&mut seed)?;
    }

    Ok((Context { map: seed.map }, order))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::TypeId;

    struct Config { greeting: String }
    struct Repo { prefix: String }
    struct Service { repo: Inject<Repo> }

    // `ComponentDescriptor` is `#[non_exhaustive]`, so even here (inside
    // oxide-core, where struct literals would still be legal) tests go
    // through the builder — it's the one real construction path and the
    // one this suite should exercise. Builder methods aren't `const fn`,
    // so descriptors are leaked at runtime rather than declared as
    // `static`s; this is test-only and has no effect on production code.
    fn repo_descriptor() -> &'static ComponentDescriptor {
        Box::leak(Box::new(
            ComponentDescriptor::builder("Repo", || TypeId::of::<Repo>(), |b| {
                let cfg = b.resolve::<Config>()?;
                b.insert(Repo { prefix: cfg.greeting.clone() });
                Ok(())
            })
            .deps(|| vec![(TypeId::of::<Config>(), "Config")])
            .build(),
        ))
    }

    fn service_descriptor() -> &'static ComponentDescriptor {
        Box::leak(Box::new(
            ComponentDescriptor::builder("Service", || TypeId::of::<Service>(), |b| {
                let repo = b.resolve::<Repo>()?;
                b.insert(Service { repo });
                Ok(())
            })
            .deps(|| vec![(TypeId::of::<Repo>(), "Repo")])
            .build(),
        ))
    }

    fn seed() -> ContextBuilder {
        let mut b = ContextBuilder::new();
        b.insert(Config { greeting: "hello".into() });
        b
    }

    #[test]
    fn builder_produces_equivalent_descriptor() {
        let d = ComponentDescriptor::builder(
            "Built",
            || TypeId::of::<Repo>(),
            |b| {
                let cfg = b.resolve::<Config>()?;
                b.insert(Repo { prefix: cfg.greeting.clone() });
                Ok(())
            },
        )
        .deps(|| vec![(TypeId::of::<Config>(), "Config")])
        .build();
        assert_eq!(d.type_name, "Built");
        assert_eq!((d.deps)().len(), 1);
        assert!(d.post_construct.is_none() && d.pre_destroy.is_none());
    }

    #[test]
    fn independent_components_construct_in_type_name_order() {
        struct Zebra;
        struct Alpha;
        // Registered Zebra first — order must still be alphabetical (deterministic).
        let zebra: &'static ComponentDescriptor = Box::leak(Box::new(
            ComponentDescriptor::builder("Zebra", || TypeId::of::<Zebra>(), |b| { b.insert(Zebra); Ok(()) }).build(),
        ));
        let alpha: &'static ComponentDescriptor = Box::leak(Box::new(
            ComponentDescriptor::builder("Alpha", || TypeId::of::<Alpha>(), |b| { b.insert(Alpha); Ok(()) }).build(),
        ));
        let (_ctx, order) = assemble(vec![zebra, alpha], ContextBuilder::new()).unwrap();
        let names: Vec<_> = order.iter().map(|d| d.type_name).collect();
        assert_eq!(names, vec!["Alpha", "Zebra"]);
    }

    #[test]
    fn constructs_in_dependency_order_regardless_of_registration_order() {
        // SERVICE registered before its dependency REPO on purpose.
        let (ctx, order) = assemble(vec![service_descriptor(), repo_descriptor()], seed()).unwrap();
        assert_eq!(ctx.get::<Service>().unwrap().repo.prefix, "hello");
        let names: Vec<_> = order.iter().map(|d| d.type_name).collect();
        assert_eq!(names, vec!["Repo", "Service"]);
    }

    #[test]
    fn missing_dependency_names_both_components() {
        let err = assemble(vec![service_descriptor()], seed()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Service") && msg.contains("Repo"), "was: {msg}");
    }

    #[test]
    fn cycle_is_reported_with_path() {
        struct A(#[allow(dead_code)] u8);
        struct B(#[allow(dead_code)] u8);
        let cyc_a: &'static ComponentDescriptor = Box::leak(Box::new(
            ComponentDescriptor::builder("A", || TypeId::of::<A>(), |_| Ok(()))
                .deps(|| vec![(TypeId::of::<B>(), "B")])
                .build(),
        ));
        let cyc_b: &'static ComponentDescriptor = Box::leak(Box::new(
            ComponentDescriptor::builder("B", || TypeId::of::<B>(), |_| Ok(()))
                .deps(|| vec![(TypeId::of::<A>(), "A")])
                .build(),
        ));
        let err = assemble(vec![cyc_a, cyc_b], ContextBuilder::new()).unwrap_err();
        assert!(matches!(err, ContextError::Cycle { .. }));
        assert!(err.to_string().contains("A -> B -> A"), "was: {err}");
    }

    #[test]
    fn duplicate_registration_is_an_error() {
        let err = assemble(vec![repo_descriptor(), repo_descriptor()], seed()).unwrap_err();
        assert!(matches!(err, ContextError::Duplicate { .. }));
    }

    #[test]
    fn inject_derefs_and_clones_share_the_same_instance() {
        let (ctx, _) = assemble(vec![repo_descriptor()], seed()).unwrap();
        let a = ctx.get::<Repo>().unwrap();
        let b = a.clone();
        assert!(std::ptr::eq(&*a, &*b));
        assert_eq!(a.prefix, "hello"); // Deref
    }
}
