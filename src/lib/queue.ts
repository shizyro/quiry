/**
 * A simple, lock-free, circular buffer-backed queue.
 *
 * Enqueue and dequeue operations run in O(1) time. The internal
 * buffer is resized dynamically to maintain efficient memory usage.
 */
export class Queue<T> implements Iterable<T> {
  protected buffer: (T | undefined)[];
  protected head: number = 0;
  protected tail: number = 0;
  protected length: number = 0;
  protected capacity: number;

  constructor(initialCapacity: number = 16) {
    if (initialCapacity <= 0) throw new Error("Initial capacity must be > 0");

    this.capacity = 1 << (32 - Math.clz32(initialCapacity - 1));
    this.buffer = new Array(this.capacity);
  }

  protected resize() {
    const newCapacity = this.capacity * 2;
    const newBuffer = new Array<T | undefined>(newCapacity);

    // Unroll the ring into a linear buffer, preserving FIFO order.
    for (let i = 0; i < this.length; i++) {
      newBuffer[i] = this.buffer[(this.head + i) & (this.capacity - 1)];
    }

    this.buffer = newBuffer;
    this.head = 0;
    this.tail = this.length;
    this.capacity = newCapacity;
  }

  /** Add an item to the back of the queue. */
  enqueue(item: T): void {
    if (this.length === this.capacity) this.resize();
    this.buffer[this.tail] = item;
    this.tail = (this.tail + 1) & (this.capacity - 1);
    this.length++;
  }

  /** Remove and return the front item. */
  dequeue(): T | undefined {
    if (this.length === 0) return undefined;
    const item = this.buffer[this.head];
    this.buffer[this.head] = undefined;
    this.head = (this.head + 1) & (this.capacity - 1);
    this.length--;
    return item;
  }

  /** Return the front item without removing it. */
  peek(): T | undefined {
    return this.length === 0 ? undefined : this.buffer[this.head];
  }

  get size(): number {
    return this.length;
  }

  isEmpty(): boolean {
    return this.length === 0;
  }

  /** Remove all items. */
  clear(): void {
    this.buffer = new Array(this.capacity);
    this.head = 0;
    this.tail = 0;
    this.length = 0;
  }

  /** Iterate over the queue front to back. */
  *[Symbol.iterator](): Iterator<T> {
    for (let i = 0; i < this.length; i++) {
      yield this.buffer[(this.head + i) & (this.capacity - 1)]!;
    }
  }

  /** Snapshot the queue as an array. */
  toArray(): T[] {
    const result = new Array<T>(this.length);
    for (let i = 0; i < this.length; i++) {
      result[i] = this.buffer[(this.head + i) & (this.capacity - 1)]!;
    }
    return result;
  }
}

/** Async-iterable, pull-based queue with backpressure. */
export class AsyncQueue<T> extends Queue<T> implements AsyncIterableIterator<T> {
  private closed = false;
  private error: unknown = null;
  private waiters: Array<{
    resolve: (value: IteratorResult<T>) => void;
    reject: (reason?: any) => void;
  }> = [];

  override enqueue(item: T): void {
    if (this.closed) throw new Error("Queue is closed");

    // If there are pending consumers, satisfy one immediately
    if (this.waiters.length > 0) {
      const { resolve } = this.waiters.shift()!;
      resolve({ value: item, done: false });
      return;
    }

    super.enqueue(item);
  }

  next(): Promise<IteratorResult<T>> {
    if (this.length > 0) return Promise.resolve({ value: super.dequeue()!, done: false });
    if (this.closed) {
      return this.error ? Promise.reject(this.error) : Promise.resolve({ value: undefined, done: true });
    }
    return new Promise<IteratorResult<T>>((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
  }

  return(): Promise<IteratorResult<T>> {
    this.close();
    return Promise.resolve({ value: undefined, done: true });
  }

  throw(error?: any): Promise<IteratorResult<T>> {
    const reason = error ?? new Error("Queue thrown");
    this.fail(reason);
    return Promise.reject(reason);
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<T> {
    return this;
  }

  /**
   * Gracefully close the queue. Pending `next()` waiters are resolved with
   * `{done: true}`; any items still buffered remain consumable until drained.
   */
  close(): void {
    if (this.closed) return;
    this.closed = true;

    for (const { resolve } of this.waiters) resolve({ value: undefined as any, done: true });
    this.waiters = [];
  }

  /**
   * Fail the queue with an error. Pending and future `next()` calls reject
   * with the given error. Buffered items are discarded.
   */
  fail(error: unknown): void {
    if (this.closed) return;
    this.closed = true;
    this.error = error;

    for (const { reject } of this.waiters) reject(error);
    this.waiters = [];
    super.clear();
  }
}

interface Deferred {
  resolve: (value?: unknown) => void;
  reject: (error: Error) => void;
}

/** A queue wrapper for deferred pull-based consumption. */
export class DeferredQueue<T = unknown> extends Queue<T> implements AsyncIterable<T> {
  #waiting: Deferred | null = null;
  #closed: boolean = false;

  override enqueue(item: T): void {
    if (this.#closed) return;
    super.enqueue(item);
    this.#waiting?.resolve();
    this.#waiting = null;
  }

  /** Drains buffered items, marks closed; pending iterator waiters resolve and the loop exits. */
  close(): void {
    if (this.#closed) return;
    this.clear();
    this.#closed = true;
    this.#waiting?.resolve();
    this.#waiting = null;
  }

  async *[Symbol.asyncIterator](): AsyncIterableIterator<T> {
    while (true) {
      while (this.size > 0) yield this.dequeue()!;
      if (this.#closed) return;

      await new Promise((resolve, reject) => {
        this.#waiting = { resolve, reject };
      });
    }
  }
}
