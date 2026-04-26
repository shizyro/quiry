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
