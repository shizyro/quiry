/**
 * A symbol used to mark a property to substitute the entire object
 * with before sending it to the remote side.
 *
 * This is used to mask objects across the wire.
 */
export const serialize: unique symbol = Symbol("quiry.serialize");

/**
 * A symbol used to call the release method on a callback.
 * Used for explicit resource management.
 */
export const release: unique symbol = Symbol("quiry.callback.release");
