# Quiry

One opinionated implementation of a transparent, type-safe IPC for worker threads and child processes in Node. It is a thin facade over message-based inter-process APIs, bridging local objects across boundaries. Designed so that the obvious way to use it is also the correct way.

```
$ npm install --save quiry
```

![Preview](https://github.com/user-attachments/assets/d4177274-e1c3-4f67-a50b-2de3e18c752c)

---

You expose an object on one side, and use it from the other side as if it were local. Methods, properties, and even generators carry across the boundary.

If it looks like a function, you call it. If it looks like a value, you read it.


## Basic Usage

```typescript
// host.ts
import Quiry from "quiry";

// spawn a worker/child_process
Quiry.fork(join(__dirname, "child.ts"));

class MathService {
  version: string = "1.0.0";

  add(a: number, b: number): number {
    return a + b;
  }

  *range(start: number, end: number) {
    for (let i = start; i <= end; i++) yield i;
  }
}

// expose a service with a unique identifier
Quiry.expose("math", MathService);

export type ServiceRegistry = {
  math: MathService;
}
```

```typescript
// child.ts
import Quiry, { ChildProcessTransport } from "quiry";
import type { ServiceRegistry } from "./host";

// attach to host, and keep a peer reference to later query exposed services
const peer = Quiry.attach<ServiceRegistry>(new ChildProcessTransport());
const math = peer.service("math"); // Remote<MathService>

console.log(await math.version); // 1.0.0
console.log(await math.add(1, 2)); // 3
for await (const n of math.range(1, 3)) {
  console.log(n); // 1, 2, 3
}
```

The proxy returned by `peer.service(...)` has the exact shape of the original interface, wrapped in an async transformer. You export and pass the service registry into the peer generic, or directly into the service callsite to override the inferred type.

```typescript
const peer = Quiry.attach<Registry>(...);
peer.service("foo"); // Inferred from Registry
peer.service<FooService>("foo"); // Type override
```

`Quiry.fork()` and `Quiry.spawn()` are convenience methods that handle transport construction. If you need more control over the worker instance, you can construct it yourself and attach manually:

```typescript
const worker = new Worker("worker.ts");
Quiry.attach(new WorkerThreadsTransport({ worker }));
```

For a more detailed showcase, make sure to check this [basic example](https://github.com/shizyro/quiry/tree/main/examples/basic);


## Streaming

Returning a single value is not always enough, and not every operation is a request/response. Streaming is returning data in chunks as it becomes available rather than waiting for the full result, this is done through [Generators](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Generator). Normally, streaming across a process boundary means building an event protocol, manually chunking messages, or batching results. Quiry lets services expose generators instead.

Streams should be pulled, not pushed. The problem is that there's no reliable way to know at runtime whether a remote method is a generator or a regular function. A separate opt-in API would feel off as well.

For that, the same proxy method is **both awaitable and async-iterable**. Whichever protocol engages depends on how you consume it first. The inferred type system narrows accordingly.

```typescript
// service can return any iterator or generator, sync or async
function* range(start: number, end: number) { ... }
```

```typescript
// the caller can opt for an async iterable
for await (const value of service.range(0, 10)) { ... }
```

If the callsite feels natural, it should do what's expected.

> Quiry uses a [credit-based flow control](https://oneflow2020.medium.com/the-history-of-credit-based-flow-control-part-1-342ec6efe23c) to solve backpressure; a fast producer shouldn't outpace a slow consumer indefinitely, growing the message queue until something gives.


## Callbacks

Functions don't survive structured cloning, but we can't pretend they don't exist; half of what we do with event emitters, request handlers, or progress reporters need functions as arguments. Workarounds are painful to deal with.

If your method accepts a callback, the caller should be able to pass one.

```typescript
await peer.service<TimerService>("timer")
  .every(1000, (tick) => console.log("tick", tick));
```

Quiry replaces functional arguments with lightweight serializable stubs, and sends them across. On the receiving end, that stub is rebuilt into a real async function that, when called, fires an invocation to the original reference across the wire, and returns the result.

However, with that, the garbage collector cannot decide on functions with no actual local [references](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Memory_management#references). And so, **callback lifetimes are explicit**. You can either pass functions inline, making them tied to the call — when the call settles, they're released automatically with no cleanup required, or, wrap that function in a **callback proxy**.  

Callback proxies are session-scoped, outliving a single call. They are released explicitly with `.release()`, or when the wrapper goes out of scope under [TC39 explicit resource management](https://github.com/tc39/proposal-explicit-resource-management). If your runtime supports [WeakRefs](https://github.com/tc39/proposal-weakrefs), the callback proxy will be released automatically at remote side when its garbage collected.

```typescript
// released when the call returns
await peer.service<JobsService>("jobs").run(jobId, (progress) => console.log(progress));

// released when you say so
const handle = peer.callback((event) => { ... });
await peer.service<StreamService>("stream").subscribe("updates", handle);
// ... later
handle.release();

// or let the runtime handle it
using handle = peer.callback((event) => { ... });
await peer.service<StreamService>("stream").subscribe("updates", handle);
// the callback is released automatically when the scope exits
```

Callbacks are always asynchronous from the remote caller’s perspective because invoking them requires an IPC round trip. Design remote methods so callback parameters may return promises.

### Returned Function Stubs

Quiry transparently handles functions that appear in **return values** from remote service methods — not just in arguments passed to them. When a service method returns a function (or a plain object containing functions), those functions are automatically translated into callback proxies that work identically to locally-defined functions.

```typescript
// example of a higher order function
listen(event: string, listener: (...args: unknown[]) => void): Unsubscribe {
  this.emitter.on(event, listener);
  return () => void this.emitter.off(event, listener);
}
```

```typescript
const off: Unsubscribe = await service.listen("foo", () => { ... });
// ... later
await off();
// the callback is released from peer side when it's no longer used
```

These returned function stubs are session-scoped on the caller side. They live for as long as the caller holds a reference, and are automatically released from remote side once the stub is **no longer referenced**, and it is reclaimed by GC. This done with Javascript's [FinalizationRegistry](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/FinalizationRegistry), which allows to eventually notify the remote peer after the local stub is collected.


## Limitations

Everything crossing a thread boundary goes through structured cloning. That comes with constraints worth knowing upfront.

Structured-cloneable data works as expected: primitives, arrays, plain objects, and other values supported by the underlying runtime transport.

Some values are intentionally limited or not supported yet:

- class instances cross the boundary as data, not as live instances with their prototype chain intact
- methods returning `this` are not useful across the boundary
- non-serializable values do not work unless provided a specific proxy mechanism for them
- functions are supported through callback proxies and returned function stubs, not through structured cloning itself

Transferables objects are supported and are collected automatically from requests. However, they are not thoroughly tested, yet. While the structured clone algorithm accepts `SharedArrayBuffer` objects, shared memory is only accessible to worker threads; child processes operate in entirely isolated memory spaces.


## Status

Single-author project, pre-1.0. The internal protocol shape is mostly stable, but the public API surface still has open decisions and may change before a stable release.
> This project is under active development. Many edge cases have not been tested. If you encounter any issues, please [open an issue](https://github.com/shizyro/quiry/issues).

All contents of this repository and its history are licensed under Apache License 2.0.