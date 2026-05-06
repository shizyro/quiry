# Quiry

One opinionated implementation of a transparent, type-safe IPC for worker threads and child processes in Node. Designed so that the obvious way to use it is also the correct way. It is a thin facade over message-based interprocess APIs, bridging local objects across boundaries.

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

class MathService {
  version: string = "1.0.0";
  
  add(a: number, b: number): number { return a + b; }
  *range(start: number, end: number) {
    for (let i = start; i <= end; i++) yield i;
  }
}

Quiry.expose("math", new MathService());
Quiry.fork(join(__dirname, "child.ts"));

export type ServiceRegistry = {
  math: MathService;
}
```

```typescript
// child.ts
import Quiry, { ChildProcessTransport } from "quiry";
import type { ServiceRegistry } from "./host";

const peer = Quiry.attach<ServiceRegistry>(new ChildProcessTransport());
const math = peer.service("math"); // RemoteServiceDefinition<MathService>

console.log(await math.version); // property access
console.log(await math.add(1, 2)); // method call
for await (const n of math.range(1, 3)) { // async iterators
  console.log(n);
}
```

The proxy returned by `peer.service(...)` has the exact shape of the original interface, wrapped in an async transformer. Typing the registry across the boundary currently has a few options — importing and passing the service type directly, augmenting the global registry, or an explicit generic at the callsite:

```typescript
peer.service<MathService>("math");
```

```typescript
declare module "quiry" {
  interface GlobalServiceRegistry {
    host: {
      math: MathService;
      // ...
    };
  }
}
```
> The namespace model was a deliberate tradeoff; it makes the framework easier to use at the cost of some type inference elegance. Better ergonomics are still being explored.


`Quiry.fork()` and `Quiry.spawn()` are convenience methods that handle transport construction. If you need more control over the worker instance, you can construct it yourself and attach manually:

```typescript
const worker = new Worker("./worker.ts");
Quiry.attach(new WorkerThreadsTransport({ worker }));
```


## Streaming

Returning a single value is not always enough, and not every operation is a request/response. Streaming is returning data in chunks (via `yield` — think [generators](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Generator)) as it becomes available rather than waiting for the full result. Normally across such boundary, this would require some of an event emitter, manual chunking over wire, or batching. None of those are particularly clean, and more importantly, none of them are how you'd write it locally.

Streams should be pulled, not pushed. The problem is that there's no reliable way to know at runtime whether a remote method is a generator or a regular function. A separate opt-in API would feel off as well.

For that, the same proxy method is **both awaitable and async-iterable**. Whichever protocol engages depends on how you consume it first. The inferred type system narrows accordingly.

```typescript
// service can return any iterator or generator, sync or async
function* range(start: number, end: number) { ... }
```

```typescript
// the caller can opt for an async iterable
for await (const value of peer.service("helpers").range(0, 10)) { ... }
```

If the callsite feels natural, it should do what's expected.

> Quiry uses a [credit-based flow control](https://oneflow2020.medium.com/the-history-of-credit-based-flow-control-part-1-342ec6efe23c) to solve backpressure; a fast producer shouldn't outpace a slow consumer indefinitely, growing the message queue until something gives.

## Callbacks

Functions don't survive structured cloning, but we can't pretend they don't exist; half of what we do with event emitters, request handlers, or progress reporters need functions as arguments. Walkarounds are painful to deal with.

If your method accepts a callback, the caller should be able to pass one.

```typescript
await peer.service<TimerService>("timer").every(1000, (tick) => {
  console.log("tick", tick);
});
```

Quiry replaces functional arguments with lightweight serializable stubs, and sends them across. On the receiving end, that stub is rebuilt into a real async function that, when called, fires an invocation to the original reference across the wire, and returns the result.

However, with that, the garbage collector cannot decide on functions with no actual local [references](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Memory_management#references). And so, **callback lifetimes are explicit**. You can either pass functions inline, making them tied to the call — when the call settles, they're released automatically with no cleanup required, or, wrap that function in a **callback proxy**.  

Callback proxies are session-scoped, outliving a single call. They are released explicitly with `.release()`, when the wrapper goes out of scope under [TC39 explicit resource management](https://github.com/tc39/proposal-explicit-resource-management), or when the session itself drains.

```typescript
// released when the call returns
await peer.service<JobsService>("jobs").run(jobId, (progress) => console.log(progress));

// released when you say so
const onEvent = peer.callback((event) => handleEvent(event));
await peer.service<StreamService>("stream").subscribe("updates", onEvent);
// ... later
onEvent.release();

// or let the runtime handle it
using onEvent = peer.callback((event) => handleEvent(event));
await peer.service<StreamService>("stream").subscribe("updates", onEvent);
// the callback is released automatically when the scope exits
```

Callbacks are always async from the caller's perspective, but that is only a constraint to account for the invocation round-trip.

## Limitations

Everything crossing a thread boundary goes through structured cloning. That comes with constraints worth knowing upfront.

- **Non-serializable return values** — methods returning `this`, class instances (the prototype chain doesn't survive, only the data), circular references, or any other non-serializable value will not work as expected. Design service methods to return plain data.
- **Non-serializable arguments** — the same applies to arguments. Functions are the one exception, handled explicitly via callback proxying.
- **Streams as arguments** — passing an iterable or a `ReadableStream` into a method is not supported yet. The direction is planned, but unsolved.
- **Transferables and `SharedArrayBuffer`** — not handled, yet.

## Status

Single-author project, pre-1.0. The internal protocol shape is mostly stable. The public API surface still has open decisions, and is not settled on.

MIT License