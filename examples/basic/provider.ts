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
}

const broker = new QuiryBroker()
  .expose("math", new MathService()) //
  .expose("greeter", new GreeterService());

export default broker;
export type AppRegistry = MappedServiceRegistry<InferServiceRegistry<typeof broker>>;
