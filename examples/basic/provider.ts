import { EventEmitter } from "node:events";

import { Broker } from "@/core/broker";
import type { InferServiceRegistry } from "@/index";

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

  async *prime(start: number = 2): AsyncIterableIterator<number> {
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

type ExampleEvents = { test: [query?: string] };
class EventService {
  readonly emitter = new EventEmitter();
  get eventNames(): string[] {
    return this.emitter.eventNames().map(String);
  }

  on<TEventName extends keyof ExampleEvents>(
    event: TEventName,
    listener: (...args: ExampleEvents[TEventName]) => void,
  ) {
    this.emitter.on(event, listener);
  }

  emit<TEventName extends keyof ExampleEvents>(
    event: TEventName,
    ...args: ExampleEvents[TEventName]
  ): boolean {
    return this.emitter.emit(event, ...args);
  }
}

const broker = new Broker()
  .expose("math", new MathService()) //
  .expose("greeter", new GreeterService())
  .expose("events", new EventService())
  .expose("timer", {
    async delay<T>(handler: (...args: any[]) => T, ms: number): Promise<T> {
      await new Promise((resolve) => setTimeout(resolve, ms));
      return handler(1, 2, 3);
    },
  });

export default broker;
export type AppRegistry = InferServiceRegistry<typeof broker>;
