import { defineConfig } from "vitest/config";
import type { UserConfig } from "vite";

const config: UserConfig = defineConfig({
  test: {
    globals: true,
    includeSource: ["./src/**/*.ts"],
  },
  resolve: {
    alias: { "~": "./src" },
  },
});

export default config;
