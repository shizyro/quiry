import * as Quiry from "~/";

import { isMainThread } from "node:worker_threads";
import { openSync, readSync, closeSync } from "node:fs";
import { join } from "node:path";

import EventEmitter from "node:events";

class GreeterService {
  greet(name: string): void {
    console.log(`\n\tHello, ${name}!\n`);
  }
}

class MathService {
  readonly pi: number = Math.PI;

  add(a: number, b: number): number {
    return a + b;
  }

  multiply(a: number, b: number): number {
    return a * b;
  }

  threshold(min: number) {
    return (value: number): boolean => {
      return value >= min;
    };
  }

  *prime(start: number = 2): Generator<number> {
    let n = Math.max(2, Math.floor(start));
    while (true) {
      if (isPrime(n)) yield n;
      n++;
    }
  }
}

function isPrime(n: number): boolean {
  if (n < 2) return false;
  if (n === 2) return true;
  if (n % 2 === 0) return false;
  for (let i = 3; i * i <= n; i += 2) {
    if (n % i === 0) return false;
  }
  return true;
}

type ExampleEvents = { foo: [query?: string]; bar: [] };
class EventService {
  readonly emitter = new EventEmitter();
  get eventNames(): string[] {
    return ["foo", "bar"];
  }

  on<TEventName extends keyof ExampleEvents>(
    event: TEventName,
    listener: (...args: ExampleEvents[TEventName]) => void,
  ) {
    this.emitter.on(event, listener);
    return () => void this.emitter.off(event, listener);
  }

  emit<TEventName extends keyof ExampleEvents>(
    event: TEventName,
    ...args: ExampleEvents[TEventName]
  ): boolean {
    return this.emitter.emit(event, ...args);
  }
}

class FileService {
  constructor(private readonly root: string) {}

  open(path: string) {
    const handle = openSync(join(this.root, path), "r");
    return {
      read: (n: number) => {
        const buffer = Buffer.alloc(n);
        const bytesRead = readSync(handle, buffer, 0, n, null);
        // (must be converted to string to survive serialization)
        return buffer.subarray(0, bytesRead).toString();
      },
      close: () => {
        closeSync(handle);
      },
    };
  }
}

export type ExampleRegistry = {
  greeter: GreeterService;
  math: MathService;
  events: EventService;
  file: FileService;
  timer: {
    delay<T>(handler: (...args: any[]) => T, ms: number): Promise<T>;
  };
};

async function bootstrap() {
  // constructor pattern
  Quiry.expose("greeter", GreeterService, {
    lifetime: Quiry.ServiceLifetime.Singleton,
  });
  Quiry.expose("file", FileService, { dependencies: [__dirname] });

  // value pattern
  Quiry.expose("math", new MathService());
  Quiry.expose("timer", {
    async delay<T>(handler: (...args: any[]) => T, ms: number): Promise<T> {
      await new Promise((resolve) => setTimeout(resolve, ms));
      return handler(1, 2, 3);
    },
  });

  // factory pattern
  Quiry.expose("events", () => new EventService(), {
    lifetime: Quiry.ServiceLifetime.Singleton,
    // must be singleton so later emitted events are not pushed into new instances
  });

  Quiry.spawn(join(__dirname, "worker.ts"));
}

if (isMainThread) {
  console.clear();
  void bootstrap();
}
