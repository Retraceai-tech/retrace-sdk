import { defineConfig } from "vitest/config";
// RETRACE_TELEMETRY=0: the SDK fires anonymous opt-out diagnostic telemetry to /internal/logs on
// configure(); unit tests must not emit it (it adds spurious network calls and perturbs fetch-count
// assertions). Disabled here for the whole suite.
//
// testTimeout/hookTimeout are raised off the 5s default for the same reason the api suite sets them
// (see packages/api/vitest.config.ts): CI runs `turbo run test` across shared+api+web+sdk-typescript
// CONCURRENTLY on one runner, so these files compete for CPU with the api suite's PGlite (WASM
// Postgres) instances. Several tests here do dynamic `import()` resolution — notably the LangChain
// adapter's "not installed" path — which costs ~4.7s even on an idle machine and 7-11s under
// contention. Against a 5s timeout that is a coin flip: the same four files
// (integration, sdk, traceparent-isolation, adapters/adapters) failed with
// "Test timed out in 5000ms" under load while passing 97/97 when the package is run alone. The work
// is not slow by nature, only starved, so a realistic ceiling is the correct fix rather than
// splitting the suite.
export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: ["src/**/*.test.ts"],
    setupFiles: ["./vitest.setup.ts"],
    env: { RETRACE_TELEMETRY: "0" },
    testTimeout: 30_000,
    hookTimeout: 30_000,
  },
});
