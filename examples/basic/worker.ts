import * as Quiry from "~";

class Solver {
  public precision: number = 6;

  evaluate(x: number): number {
    return Number((Math.sin(x) + Math.cos(x)).toFixed(this.precision));
  }

  *iterate(start: number): Generator<number> {
    let x = start;
    while (true) {
      x = this.evaluate(x);
      yield x;
    }
  }

  within(tolerance: number): (value: number) => boolean {
    return (value) => Math.abs(value) <= tolerance;
  }
}

Quiry.attach(new Quiry.WorkerThreadsTransport());

Quiry.expose("solver", new Solver());
Quiry.expose("greeter", {
  greet(name: string): void {
    console.log(`\n\tHello, ${name}!\n`);
  },
});
Quiry.expose("timer", {
  async delay<T>(handler: (...args: any[]) => T, ms: number): Promise<T> {
    await new Promise((resolve) => setTimeout(resolve, ms));
    return handler(1, 2, 3);
  },
});

Quiry.on("peer-disconnected", () => process.exit(0));

export type Registry = {
  solver: Solver;
  greeter: {
    greet(name: string): void;
  };
  timer: {
    delay<T>(handler: (...args: any[]) => T, ms: number): Promise<T>;
  };
};
