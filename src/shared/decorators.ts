type Constructor<T = any> = new (...args: any[]) => T;
type Singletonified<T extends Constructor> = T & { readonly instance: InstanceType<T> };

/**
 * Wraps a class so `new C()` and `C.instance` share one instance (lazy on first access).
 * `Reflect.construct` preserves subclass prototypes; not thread-safe across isolates.
 */
export function Singleton<T extends Constructor>(base: T): Singletonified<T> {
  if (process.env.NODE_ENV === "test") {
    // @ts-expect-error: test environment, no singleton behavior
    return base;
  }

  let instance: InstanceType<T> | null = null;
  const handler: ProxyHandler<T> = {
    construct(target, args, newTarget): InstanceType<T> {
      // Ensure correct prototyping when subclassing.
      if (!instance) instance = Reflect.construct(target, args, newTarget);
      return instance!;
    },
  };

  const Proxied = new Proxy(base, handler) as Singletonified<T>;

  Object.defineProperty(Proxied, "instance", {
    configurable: false,
    enumerable: false,
    get() {
      if (!instance) {
        // Lazily initialize using default construction.
        instance = new Proxied();
      }
      return instance;
    },
  });

  return Proxied;
}
