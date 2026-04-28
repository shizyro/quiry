import { EventEmitter } from "node:events";

import { QuiryBroker, type InferServiceRegistry } from "@/core/broker";
import type { MappedServiceRegistry } from "@/interface/transformers";

class GreeterService {
  greet(name: string): void {
    console.log(`\n\tHello, ${name}!\n`);
  }
}

class MathService {
  add(a: number, b: number): number {
    return a + b;
  }

  multiply(a: number, b: number): number {
    return a * b;
  }

  async *count(start: number, end: number): AsyncIterableIterator<number> {
    for (let i = start; i < end; i++) {
      yield i;
      // Yield back to the microtask queue so incoming CANCEL
      // packets can be processed between emissions.
      await new Promise<void>((r) => setTimeout(r, 0));
    }
  }
}

type ExampleEvents = { test: [query?: string] };
class EventService {
  readonly emitter = new EventEmitter();

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

const broker = new QuiryBroker()
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
export type AppRegistry = MappedServiceRegistry<InferServiceRegistry<typeof broker>>;
