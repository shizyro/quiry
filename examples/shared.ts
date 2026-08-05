export { join } from "node:path";
export function log(...data: unknown[]): void {
  console.log(`\u001b[2m${new Date().toISOString()}\u001b[22m`, ...data);
}
