import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { SDK_VERSION } from "./version.js";

describe("SDK version", () => {
  it("matches package.json (what npm publishes), so telemetry and x-retrace-client never report a stale version", () => {
    const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8")) as { version: string };
    expect(SDK_VERSION).toBe(pkg.version);
  });
});
