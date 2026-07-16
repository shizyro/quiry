/**
 * A symbol used to mark a property to assign a {@link Serializer} for a specific
 * data type. Serializers are used to marshal data to and from the remote side,
 * in a way that bypasses the structured cloning algorithm, and allows custom
 * serialization logic to be applied.
 *
 * For full lint support, this should be used in a class that implements
 * {@link Serializable} interface with its marker symbol ({@link opaque}).
 *
 * @example
 * ```typescript
 * class MyClass implements Quiry.Serializable {
 *  [Quiry.opaque]!: never // mark for compile-time detection
 *  static readonly [Quiry.serializer]: Serializer = ...
 *  static {
 *     // announce and register the serializer
 *     Quiry.registerSerializer(this, import.meta.url)
 *   }
 *  }
 * ```
 */
export const serializer: unique symbol = Symbol("quiry.serialize");

/**
 * A nominal marker used to indicate values that should be preserved as-is
 * during remote type transformation.
 */
export const opaque: unique symbol = Symbol("quiry.opaque");

/**
 * A symbol used to mark a property to override the entire object with before
 * sending it to the remote side. Unlike {@link serializer}, the overriden value
 * is not expected to be modified or rebuilt at the remote side.
 */
export const override: unique symbol = Symbol("quiry.override");

/**
 * A symbol used to identify an quiry structured value. Used for debugging.
 */
export const identifier: unique symbol = Symbol("quiry.identifier");

/**
 * A symbol used to call the release method on a callback.
 * Used for explicit resource management.
 */
export const release: unique symbol = Symbol("quiry.callback.release");

/**
 * A symbol used to assign control options to a call request before it is sent.
 */
export const control: unique symbol = Symbol("quiry.control");
