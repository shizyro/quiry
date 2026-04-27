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

const broker = new QuiryBroker()
  .expose("math", new MathService()) //
  .expose("greeter", new GreeterService());

export default broker;
export type AppRegistry = MappedServiceRegistry<InferServiceRegistry<typeof broker>>;
