import * as Quiry from "~";
import { log, join } from "../shared";

import type { Wallet } from "./worker";
import { Money } from "./money"; // must be imported on both sides, non type-only

async function main() {
  const peer = Quiry.spawn(join(import.meta.dirname, "worker.ts"));
  const wallet = peer.remote<Wallet>("wallet");

  const opening = await wallet.balance;
  // `opening` is a local reconstructed instance; no need to call `.format()` remotely
  log(`opening balance: ${opening.format()}`);

  const after = await wallet.deposit(new Money(25_50, "EUR"));
  log(`after depositing 25.50 EUR: ${after.format()}`);

  /**
   * To contrast, `Receipt` has the exact same shape as `Money`, but no registered serializer.
   * Class instances aren't silently flattened to plain data on the wire — they're rejected
   * outright, with a clear error rather than a value that's quietly missing its methods.
   */
  await wallet
    .receiptFor(new Money(25_50, "EUR")) //
    .catch((error: unknown) => {
      if (error instanceof Quiry.QuiryError) {
        log(`receipt retrieval rejected: "${error.message}" [${Quiry.WireStatus[error.code]}]`);
        return;
      }

      throw error;
    });

  /**
   * A futher contrast, `rawReceiptFor()` returns a plain object instead of a class instance.
   * Plain objects are serialized as-is, without any special handling; their properties are
   * directly copied to the wire, and their methods are proxied through Quiry.
   *
   * Note the return is a structurally-similar remote object; you'd want to treat it as such,
   * rather than as a local object.
   */
  const receipt = await wallet.rawReceiptFor(new Money(25_50, "EUR"));
  log(`raw receipt for 25.50 EUR: ${await receipt.format()}`); // (awaited)

  await peer.close();
}

main().catch(console.error);
