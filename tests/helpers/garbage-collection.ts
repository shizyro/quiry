type RunGCPressureOptions = {
  batchSize?: number;
  intervalMs?: number;
  releaseDelayMs?: number;
  signal?: AbortSignal;
};

type GCPressureHandle = {
  stop: () => void;
};

/**
 * Generates artificial memory pressure to provoke garbage collection cycles.
 * This works by repeatedly allocating a large number of short-lived objects
 * and then releasing references, making them eligible for collection.
 */
export function runGCPressure(options: RunGCPressureOptions = {}): GCPressureHandle {
  const { batchSize = 1e6, intervalMs = 1000, releaseDelayMs = 100, signal } = options;

  let data: Array<{ i: number }> = [];
  let stopped = false;

  let intervalId: ReturnType<typeof setInterval> | null = null;
  let timeoutId: ReturnType<typeof setTimeout> | null = null;

  const allocate = () => {
    for (let i = 0; i < batchSize; i++) {
      data.push({ i });
    }
  };

  const release = () => {
    data = [];
  };

  const tick = () => {
    if (stopped) return;
    allocate();
    // clear any previous pending release to avoid overlap
    if (timeoutId !== null) clearTimeout(timeoutId);
    timeoutId = setTimeout(() => !stopped && release(), releaseDelayMs);
  };

  const stop = () => {
    if (stopped) return;
    stopped = true;

    if (intervalId !== null) {
      clearInterval(intervalId);
      intervalId = null;
    }

    if (timeoutId !== null) {
      clearTimeout(timeoutId);
      timeoutId = null;
    }

    data = [];
  };

  // start loop
  intervalId = setInterval(tick, intervalMs);

  // wire abort once
  if (signal) {
    if (signal.aborted) stop();
    else signal.addEventListener("abort", () => stop(), { once: true });
  }

  return { stop };
}
