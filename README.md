# Quiry

An implementation of a transparent, type-safe IPC for worker threads and child processes in Node. It is a thin facade over message-based interprocess APIs, bridging local objects across boundaries. Designed so that the obvious way to use it is also the correct way.

```
$ pnpm i jsr:@shizyro/quiry
```

![Preview](https://github.com/user-attachments/assets/551f6db6-d1df-4b56-a1da-f01ec25b66a9)

---

You expose an object on <u>either</u> sides, and use it from the other side as if it were local. Methods, properties, and even generators carry across the boundary.

If it looks like a function, you call it. If it looks like a value, you read or assign to it.

## Basic Usage

```typescript
// main.ts
import * as Quiry from "quiry";
import { fork } from "node:child_process";

// create a child process, and wrap to register
const child = fork("child.ts");
Quiry.wrap(child);

class MathService {
  readonly version: string = "1.0.0";

  add(a: number, b: number): number {
    return a + b;
  }

  *count(start: number, end: number): Generator<number> {
    for (let i = start; i <= end; i++) yield i;
  }
}

// expose an object with a unique identifier
Quiry.expose("math", new MathService());

export type RemoteRegistry = {
  math: MathService;
};
```

```typescript
// child.ts
import * as Quiry from "quiry";
import type { RemoteRegistry } from "./main";

// create an inter-process transport. in this case, it hooks to parent by default
const transport = new Quiry.ChildProcessTransport();
// attach to local registry, and keep a peer reference to later query exposed objects
const peer = Quiry.attach<RemoteRegistry>(transport);

// now, you can access remote objects from that transport
const math = peer.remote("math"); // Remote<MathService>

console.log(await math.version); // -> 1.0.0
console.log(await math.add(1, 2)); // -> 3
for await (const n of math.count(1, 3)) {
  console.log(n); // -> 1, 2, 3
}
```

The proxy returned by `peer.remote(...)` has the exact shape of the original interface, wrapped in an async transformer. You export and pass the remote registry into the peer generic, or directly into the proxy callsite to override the inferred type.

```typescript
const peer = Quiry.attach<Registry>(...);
peer.remote("foo"); // Remote<Registry["foo"]> [inferred]
peer.remote<FooService>("foo"); // Remote<FooService> [type override]
```

Note there is `Quiry.fork()` and `Quiry.spawn()`, which are convenience methods that handle transport construction. If you need more control over the worker instance, you can construct it yourself and attach manually:

```typescript
const worker = new Worker("worker.ts");
Quiry.attach(new WorkerThreadsTransport(worker));
```

For a more thorough showcase, make sure to check this [basic example](./examples/basic).

## Streaming

Returning a single value is not always enough, and not every operation is a request/response. Streaming is returning data in chunks as it becomes available rather than waiting for the full result, this is done through [Generators](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Generator). Normally, streaming across a process boundary means building an event protocol, manually chunking messages, or batching results. Quiry lets you expose generators as they are instead.

Streams should be pulled, not pushed. The problem is that there's no reliable way to know at runtime whether a remote method is a generator or a regular function. A separate opt-in API would feel off as well.

For that, the same proxy method is **both awaitable and async-iterable**. Whichever protocol engages depends on how you consume it first. The inferred type system narrows accordingly.

```typescript
// remote objects can return any iterator or generator, sync or async
function* range(start: number, end: number) { ... }
```

```typescript
// the caller can opt for an async iterable
for await (const value of proxy.range(0, 10)) { ... }
```

If the callsite feels natural, it should do what's expected.

> Quiry uses a [credit-based flow control](https://oneflow2020.medium.com/the-history-of-credit-based-flow-control-part-1-342ec6efe23c) to solve backpressure; a fast producer shouldn't outpace a slow consumer indefinitely, growing the message queue until something gives.

## Callbacks

Functions don't survive structured cloning, but we can't pretend they don't exist; half of what we do with event emitters, request handlers, or progress reporters need functions as arguments. Workarounds are painful to deal with.

If your method accepts a callback, the caller should be able to pass one.

```typescript
await peer.remote<TimerService>("timer").delay(1000, () => { ... });
```

Quiry replaces functional arguments with lightweight serializable stubs, and sends them across. On the receiving end, that stub is rebuilt into a real async function that, when called, fires an invocation to the original reference across the wire, and returns the result.

However, with that, the garbage collector cannot decide on functions with no actual local [references](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Memory_management#references). And so, **callback lifetimes are explicit**. You can either pass functions inline, making them tied to the call — when the call settles, they're released automatically with no cleanup required, or, wrap that function in a **callback proxy**.

Callback proxies are session-scoped, outliving a single provocation. They are released explicitly with `[Quiry.release]()`, or when the wrapper goes out of scope under [TC39 explicit resource management](https://github.com/tc39/proposal-explicit-resource-management). If your runtime supports [WeakRefs](https://github.com/tc39/proposal-weakrefs), the callback proxy will be released automatically at remote side when its garbage collected.

```typescript
// released when the call returns
await peer.remote<JobsService>("jobs").run(jobId, (progress) => { ... });

// released when you say so
const handle = peer.callback((event) => { ... });
await peer.remote<StreamService>("stream").subscribe("updates", handle);
// ... later
handle[Quiry.release]();

// or let the runtime handle it
using handle = peer.callback((event) => { ... });
await peer.remote<StreamService>("stream").subscribe("updates", handle);
// [the callback is automatically released when the scope exits]
```

Callbacks are **always asynchronous from the remote caller's perspective** because invoking them requires an IPC round trip. Design remote methods so callback parameters may return promises.

### Returned Function Stubs

Quiry transparently handles functions that appear in **return values** from remote object methods — not just in arguments passed to them. When a method returns a function (or a plain object containing functions), those functions are automatically translated into callback proxies that work identically to locally-defined functions.

```typescript
// example of a higher order function
listen(event: string, listener: (...args: unknown[]) => void): Unsubscribe {
  this.emitter.on(event, listener);
  return () => void this.emitter.off(event, listener);
}
```

```typescript
const off = await proxy.listen("foo", () => { ... }); // Remote<Unsubscribe>
// ... later
await off();
// [the callback is automatically released from peer side when it's no longer used]
```

These returned function stubs are session-scoped on the caller side. They live for as long as the caller holds a reference, and are automatically released from remote side once the stub is **no longer referenced**, and it is reclaimed by GC. This is also done through Javascript's [FinalizationRegistry](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/FinalizationRegistry), which allows to eventually notify the remote peer after the local stub is collected.

## Request Control

Remote calls can be slow, or simply outlive the caller's interest. An `AbortSignal` doesn't cross the boundary on its own — signals aren't serializable, and neither is intent by itself. Quiry threads control through the same proxy call, via a symbol rather than an extra parameter that would clutter your call signature:

```typescript
await proxy.method[Quiry.control](signal)(...args);
```

That's the general shape. Cancellation surfaces as its own status, rather than a generic rejection.

```typescript
const signal = AbortSignal.timeout(5000);
await peer
  .remote<FileService>("file")
  .open[Quiry.control](signal)("data.txt")
  .catch((error) => {
    if (error instanceof QuiryError && error.code === Quiry.WireStatus.ABORTED) {
      console.log("Call aborted; timed out");
      return;
    }

    throw error; // rethrow any other error
  });
```

Aborting rejects the call locally and tells the peer to stop working on it. On the exposed side, `Quiry.signal()` returns the ambient signal, via [asynchronous context tracking](https://nodejs.org/api/async_context.html#asynchronous-context-tracking), for the call currently executing, so long-running methods can cooperate instead of running to completion regardless:

```typescript
async function longRunningTask() {
  const signal = Quiry.signal();
  while (!signal?.aborted) { ... }
}
```

Streams honor the same signal for their entire lifetime, not just the initial request.

## Custom Serialization

Structured cloning works as expected for most data types, but, it only outputs plain data; class instances cross the boundary as a plain object with no related prototype chain.

However, you can define custom serialization strategies for specific class instances and other complex data that would not work as they are.

```typescript
class Money {
  constructor(
    public readonly cents: number,
    public readonly currency: string,
  ) {}

  // define custom serialization strategy
  static readonly [Quiry.serializer] = {
    serialize: (value: Money) => ({ cents: value.cents, currency: value.currency }),
    deserialize: (data: { cents: number; currency: string }) => new Money(data.cents, data.currency),
  } satisfies Quiry.Serializer;

  // (this is called only once when the class is first loaded)
  static {
    // announce and register the serializer
    Quiry.registerSerializer(this, import.meta.url);
  }
}
```

Once registered, instances of that class crosses like any other value, and comes out at the other side as a constructed instance, not a shell of one.

```typescript
await peer.remote<WalletService>("wallet").charge(new Money(500, "EUR"));
```

Registration happens when the class loads, so both sides need the file itself imported at runtime — a type-only import never runs the class body, and the peer won't know what to reconstruct. Please check [this example](./examples/custom-serialization) for a more advanced showcase.

> If your build renames classes during minification, or two classes would otherwise collide, pass an explicit `id` within the serializer body instead of relying on the derived one.

## Limitations

Everything crossing a thread boundary goes through structured cloning. That comes with constraints worth knowing upfront.

Structured-cloneable data works as expected: primitives, arrays, plain objects, and other values supported by the underlying runtime transport.

Some values are intentionally limited or not supported yet:

- class instances cross the boundary as plain data by default — register a [custom serializer](#custom-serialization) to preserve them as live instances instead
- methods returning `this` are not useful across the boundary
- non-serializable values do not work unless provided a specific proxy mechanism for them
- remote transformer can't correctly type methods where a generic type parameter correlates two or more arguments — this is a static TypeScript limitation, and requires a manual override per affected method. See [support for correlated union types](https://github.com/microsoft/TypeScript/issues/30581)
- streaming only flows one way — a returned generator becomes a stream, a generator passed as an argument does not

Transferables objects are supported and are collected automatically from requests. While the structured clone algorithm accepts `SharedArrayBuffer` objects, shared memory is only accessible to worker threads; child processes operate in entirely isolated memory spaces.

## Status

Single-author project, pre-1.0. The internal protocol shape is mostly stable, but the public API surface still has open decisions and may change before a stable release.

> This project is under active development. Many edge cases have not been tested. If you encounter any issues, please [open an issue](https://github.com/shizyro/quiry/issues).

All contents of this repository and its history are licensed under Apache License 2.0.
