import * as Quiry from "~";
import { Money, Receipt } from "./money";

class Wallet {
  private readonly holding: Money[];

  constructor(...cash: Money[]) {
    this.holding = cash;
  }

  get balance(): Money {
    return this.holding.reduce((acc, curr) => acc.add(curr), new Money(0, "EUR"));
  }

  deposit(amount: Money): Money {
    this.holding.push(amount);
    return this.balance;
  }

  /**
   * Returns a plain-shaped class instance with no registered serializer, for contrast.
   *
   * This will throw an error when called, as `Receipt` has a prototype chain that is
   * not serializable.
   */
  receiptFor(amount: Money): Receipt {
    return new Receipt(amount.cents, amount.currency);
  }

  /**
   * Returns a plain object instead. This, in contrast, does not have a prototype chain
   * that has to be serialized — it can be proxied right away as it is with no issue.
   *
   * However, this has to be treated as the typical remote object; it will not have a
   * reconstructible local implementation and all methods must be called remotely.
   */
  rawReceiptFor(amount: Money): typeof Receipt.prototype {
    return {
      cents: amount.cents,
      currency: amount.currency,
      format: () => `receipt for ${(amount.cents / 100).toFixed(2)} ${amount.currency}`,
    };
  }
}

Quiry.attach(new Quiry.WorkerThreadsTransport());
Quiry.expose("wallet", new Wallet(new Money(10_000, "EUR")));

Quiry.on("peer-disconnected", () => process.exit(0));

export type { Wallet };
