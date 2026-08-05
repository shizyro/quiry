import * as Quiry from "~";

/**
 * [IMPORTANT]
 * It is recommended that classes with custom serializers always implement
 * {@link Quiry.Serializable}, as it marks its type as "opaque"; meaning it
 * should always reflect at remote side as-is (no type transformations).
 */

export class Money implements Quiry.Serializable {
  readonly [Quiry.opaque]!: never; // mark for compile-time detection

  constructor(
    public readonly cents: number,
    public readonly currency: string,
  ) {}

  add(other: Money): Money {
    if (other.currency !== this.currency) {
      throw new Error(`Currency mismatch: ${this.currency} vs ${other.currency}`);
    }
    return new Money(this.cents + other.cents, this.currency);
  }

  format(): string {
    return `${(this.cents / 100).toFixed(2)} ${this.currency}`;
  }

  /**
   * Defines how to turn this class into a wire-safe plain data, and back.
   *
   * The serialized shape only needs to carry what `deserialize` needs to
   * rebuild an equivalent instnace, not every internal detail.
   */
  static readonly [Quiry.serializer] = {
    serialize: (value: Money) => ({ cents: value.cents, currency: value.currency }),
    deserialize: (data: { cents: number; currency: string }) => new Money(data.cents, data.currency),
  } satisfies Quiry.Serializer;

  static {
    /**
     * Registration happens once when this module is loaded.
     *
     * Both sides of the boundary must import this file (not `import type`)
     * so the class body  — and this registration — actually runs on both ends.
     */
    Quiry.registerSerializer(this, import.meta.url);
  }
}

/**
 * This class is used to contrast with {@link Money} in what crossing the
 * boundary looks like without a custom serialization strategy.
 */
export class Receipt {
  constructor(
    public readonly cents: number,
    public readonly currency: string,
  ) {}

  // This is reflected as a proxy promise, not a local method
  format(): string {
    return `receipt for ${(this.cents / 100).toFixed(2)} ${this.currency}`;
  }
}
