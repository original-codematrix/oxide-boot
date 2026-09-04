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

impl<T> Inject<T> {
    /// The underlying `Arc`, for the one case `Deref` cannot serve: coercing
    /// a component to `Arc<dyn Trait>`. Used by the code `#[component]`
    /// generates for `provides(...)`.
    pub fn into_arc(self) -> Arc<T> {
        self.0
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
    /// Shutdown slot: `pre_destroy` hooks run ordered by `(destroy_order
    /// ASC, then reverse construction order)`. Framework convention: `0` is
    /// normal components, torn down among themselves in reverse
    /// construction order (the historical default, unchanged); `100` is
    /// reserved for telemetry flush, which must outlive every
    /// span/metric-producing component so it can still observe their
    /// shutdown. Values in between are free for starters to place
    /// themselves relative to those two anchors.
    pub destroy_order: i32,
}

inventory::collect!(ComponentDescriptor);

/// Registers one component as an implementation of one trait, so it can be
/// resolved as `Arc<dyn Trait>` and collected into `Vec<Arc<dyn Trait>>`.
///
/// The container is keyed by `TypeId`, which means a component is normally
/// only reachable under its own concrete type: two implementations of the
/// same trait are two unrelated entries, and there is no way to ask for "all
/// of them". A binding is the seam that fixes that without giving up the
/// concrete path -- `ctx.get::<EnglishGreeter>()` keeps working, and
/// `ctx.get_all::<dyn Greeter>()` now works too.
///
/// The stored value is an `Arc<dyn Trait>` boxed inside `Arc<dyn Any>`: the
/// coercion from the concrete type has to happen where that type is known,
/// which is in the generated `bind` closure, not here.
#[derive(Debug)]
#[non_exhaustive]
pub struct TraitBinding {
    pub trait_name: &'static str,
    pub component_name: &'static str,
    /// `TypeId::of::<dyn Trait>()`.
    pub trait_id: fn() -> TypeId,
    /// `TypeId::of::<Concrete>()`; used to order bindings deterministically
    /// and to report which component a failed binding came from.
    pub component_id: fn() -> TypeId,
    /// Resolves the concrete component from the context and returns it as an
    /// erased `Arc<Arc<dyn Trait>>`. `None` when the component is absent,
    /// which `assemble` treats as a hard error -- a binding always names a
    /// component that was registered alongside it.
    pub bind: fn(&Context) -> Option<Arc<dyn Any + Send + Sync>>,
}

impl TraitBinding {
    pub const fn new(
        trait_name: &'static str,
        component_name: &'static str,
        trait_id: fn() -> TypeId,
        component_id: fn() -> TypeId,
        bind: fn(&Context) -> Option<Arc<dyn Any + Send + Sync>>,
    ) -> Self {
        Self { trait_name, component_name, trait_id, component_id, bind }
    }
}

inventory::collect!(TraitBinding);

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
                destroy_order: 0,
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

    /// Sets the shutdown slot. Default `0` (normal components, reverse
    /// construction order among themselves). `100` is the framework
    /// convention for telemetry flush — it must outlive every
    /// span/metric-producing component, so it always runs last regardless
    /// of construction order. Values in between are free for starters.
    pub const fn destroy_order(mut self, order: i32) -> Self {
        self.inner.destroy_order = order;
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

    /// Inserts `value`, keyed by `TypeId::of::<T>()`.
    ///
    /// **Invariant, not enforced here:** inserting the same `T` twice
    /// silently overwrites the first value. This is safe in practice because
    /// the realistic source of a duplicate — two [`ComponentDescriptor`]s
    /// registered for the same type — is already rejected earlier, in
    /// [`assemble`]'s dedup-by-`type_id` pass (`ContextError::Duplicate`,
    /// covered by the `duplicate_registration_is_an_error` test below),
    /// before any `construct` closure (and therefore any `insert`) runs.
    /// `insert` itself stays infallible so every `construct` closure across
    /// the workspace (hand-written and macro-generated) doesn't need to
    /// thread a `Result` through a call that, given that upstream guarantee,
    /// cannot observe a real collision.
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
///
/// `Clone`: the map is `Arc`-wrapped, so cloning is O(1) (one refcount bump,
/// not a deep copy) — needed so a context-aware `#[component]` hook (see
/// `oxide_macros::component`) can be handed an owned `Context` to move into
/// its `'static` hook future, since `HookFn`'s future cannot borrow the
/// `&Context` the hook is called with. This clone must stay confined to
/// that transient future: storing a cloned `Context` in a component's own
/// *field* would create an `Arc` reference cycle (`Context` owns every
/// component, so a component owning a clone of `Context` owns itself,
/// transitively) and leak the whole context forever. Nothing in this crate
/// or the macro does that — the only clone site is the momentary one inside
/// a hook's future.
#[derive(Clone)]
pub struct Context {
    map: Arc<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
    /// Keyed by `TypeId::of::<dyn Trait>()`; each entry holds the erased
    /// `Arc<dyn Trait>` handles contributed by [`TraitBinding`]s, in a
    /// deterministic order.
    traits: Arc<HashMap<TypeId, Vec<Arc<dyn Any + Send + Sync>>>>,
}

impl Context {
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<Inject<T>> {
        self.map.get(&TypeId::of::<T>()).and_then(|any| Arc::clone(any).downcast::<T>().ok()).map(Inject)
    }

    /// Every component registered as an implementation of `T`, where `T` is
    /// an unsized trait object type: `ctx.get_all::<dyn Greeter>()`.
    ///
    /// Returns an empty vector when nothing implements it -- "no
    /// implementations" is a legitimate state for an extension point, and
    /// callers that need at least one should say so themselves.
    ///
    /// Order is stable across runs (bindings are sorted by component type
    /// name), so a chain of handlers built from this does not silently
    /// reorder itself between boots.
    pub fn get_all<T: ?Sized + Send + Sync + 'static>(&self) -> Vec<Arc<T>> {
        self.traits
            .get(&TypeId::of::<T>())
            .map(|entries| {
                entries
                    .iter()
                    // Each entry is an `Arc<Arc<dyn T>>`; the inner Arc is
                    // what callers want, so clone it out of the box.
                    .filter_map(|any| Arc::clone(any).downcast::<Arc<T>>().ok())
                    .map(|boxed| Arc::clone(&*boxed))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The single implementation of `T`, or `None` when there are none or
    /// more than one. Ambiguity returns `None` rather than picking one:
    /// silently choosing between two implementations is how a wrong
    /// dependency reaches production unnoticed.
    pub fn get_one<T: ?Sized + Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        let mut all = self.get_all::<T>();
        if all.len() == 1 { all.pop() } else { None }
    }

    /// How many components implement `T`. Useful for a startup log line, and
    /// for telling "none registered" apart from "several" after `get_one`
    /// returns `None`.
    pub fn count_of<T: ?Sized + Send + Sync + 'static>(&self) -> usize {
        self.traits.get(&TypeId::of::<T>()).map_or(0, Vec::len)
    }
}

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("component_count", &self.map.len())
            .field("trait_count", &self.traits.len())
            .finish_non_exhaustive()
    }
}

/// Builds the context: topologically sorts descriptors by their dependencies,
/// constructs every component exactly once, fails fast on missing deps,
/// duplicates and cycles. Returns the construction order for lifecycle hooks.
pub fn assemble(
    descriptors: Vec<&'static ComponentDescriptor>,
    seed: ContextBuilder,
) -> Result<(Context, Vec<&'static ComponentDescriptor>), ContextError> {
    assemble_with_bindings(descriptors, seed, inventory::iter::<TraitBinding>.into_iter().collect())
}

/// [`assemble`] with the trait bindings passed explicitly, so tests can
/// exercise binding behaviour without registering into the process-global
/// inventory (which leaks between tests in the same binary).
pub fn assemble_with_bindings(
    descriptors: Vec<&'static ComponentDescriptor>,
    mut seed: ContextBuilder,
    bindings: Vec<&'static TraitBinding>,
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

    // Bindings run after every component exists: a binding resolves its
    // component out of the finished map, so it cannot run during
    // construction.
    let base = Context { map: Arc::new(seed.map), traits: Arc::new(HashMap::new()) };

    let mut bindings = bindings;
    // Sorted by the component's type name so `get_all` returns a stable
    // order across runs. Without this the order follows link order, which
    // changes when an unrelated crate is added.
    bindings.sort_by_key(|b| (b.trait_name, b.component_name));

    let mut traits: HashMap<TypeId, Vec<Arc<dyn Any + Send + Sync>>> = HashMap::new();
    for b in bindings {
        // A binding is emitted next to its component, so a component that is
        // not in the context means the two were separated -- a linking
        // mistake, not a configuration choice. Failing here beats an
        // extension point that is silently one implementation short.
        let bound = (b.bind)(&base)
            .ok_or(ContextError::MissingDependency { component: b.trait_name, dependency: b.component_name })?;
        traits.entry((b.trait_id)()).or_default().push(bound);
    }

    Ok((Context { traits: Arc::new(traits), ..base }, order))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::TypeId;

    struct Config {
        greeting: String,
    }
    struct Repo {
        prefix: String,
    }
    struct Service {
        repo: Inject<Repo>,
    }

    // `ComponentDescriptor` is `#[non_exhaustive]`, so even here (inside
    // oxide-core, where struct literals would still be legal) tests go
    // through the builder — it's the one real construction path and the
    // one this suite should exercise. Builder methods are `const fn`, so
    // descriptors are plain `static`s again, same as before the builder
    // existed.
    static REPO: ComponentDescriptor = ComponentDescriptor::builder("Repo", TypeId::of::<Repo>, |b| {
        let cfg = b.resolve::<Config>()?;
        b.insert(Repo { prefix: cfg.greeting.clone() });
        Ok(())
    })
    .deps(|| vec![(TypeId::of::<Config>(), "Config")])
    .build();

    static SERVICE: ComponentDescriptor = ComponentDescriptor::builder("Service", TypeId::of::<Service>, |b| {
        let repo = b.resolve::<Repo>()?;
        b.insert(Service { repo });
        Ok(())
    })
    .deps(|| vec![(TypeId::of::<Repo>(), "Repo")])
    .build();

    fn seed() -> ContextBuilder {
        let mut b = ContextBuilder::new();
        b.insert(Config { greeting: "hello".into() });
        b
    }

    #[test]
    fn builder_produces_equivalent_descriptor() {
        let d = ComponentDescriptor::builder("Built", TypeId::of::<Repo>, |b| {
            let cfg = b.resolve::<Config>()?;
            b.insert(Repo { prefix: cfg.greeting.clone() });
            Ok(())
        })
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
        static ZEBRA: ComponentDescriptor = ComponentDescriptor::builder("Zebra", TypeId::of::<Zebra>, |b| {
            b.insert(Zebra);
            Ok(())
        })
        .build();
        static ALPHA: ComponentDescriptor = ComponentDescriptor::builder("Alpha", TypeId::of::<Alpha>, |b| {
            b.insert(Alpha);
            Ok(())
        })
        .build();
        let (_ctx, order) = assemble(vec![&ZEBRA, &ALPHA], ContextBuilder::new()).unwrap();
        let names: Vec<_> = order.iter().map(|d| d.type_name).collect();
        assert_eq!(names, vec!["Alpha", "Zebra"]);
    }

    #[test]
    fn constructs_in_dependency_order_regardless_of_registration_order() {
        // SERVICE registered before its dependency REPO on purpose.
        let (ctx, order) = assemble(vec![&SERVICE, &REPO], seed()).unwrap();
        assert_eq!(ctx.get::<Service>().unwrap().repo.prefix, "hello");
        let names: Vec<_> = order.iter().map(|d| d.type_name).collect();
        assert_eq!(names, vec!["Repo", "Service"]);
    }

    #[test]
    fn missing_dependency_names_both_components() {
        let err = assemble(vec![&SERVICE], seed()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Service") && msg.contains("Repo"), "was: {msg}");
    }

    #[test]
    fn cycle_is_reported_with_path() {
        struct A(#[allow(dead_code)] u8);
        struct B(#[allow(dead_code)] u8);
        static CYC_A: ComponentDescriptor = ComponentDescriptor::builder("A", TypeId::of::<A>, |_| Ok(()))
            .deps(|| vec![(TypeId::of::<B>(), "B")])
            .build();
        static CYC_B: ComponentDescriptor = ComponentDescriptor::builder("B", TypeId::of::<B>, |_| Ok(()))
            .deps(|| vec![(TypeId::of::<A>(), "A")])
            .build();
        let err = assemble(vec![&CYC_A, &CYC_B], ContextBuilder::new()).unwrap_err();
        assert!(matches!(err, ContextError::Cycle { .. }));
        assert!(err.to_string().contains("A -> B -> A"), "was: {err}");
    }

    #[test]
    fn duplicate_registration_is_an_error() {
        let err = assemble(vec![&REPO, &REPO], seed()).unwrap_err();
        assert!(matches!(err, ContextError::Duplicate { .. }));
    }

    #[test]
    fn inject_derefs_and_clones_share_the_same_instance() {
        let (ctx, _) = assemble(vec![&REPO], seed()).unwrap();
        let a = ctx.get::<Repo>().unwrap();
        let b = a.clone();
        assert!(std::ptr::eq(&*a, &*b));
        assert_eq!(a.prefix, "hello"); // Deref
    }
}
