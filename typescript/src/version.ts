/**
 * The SDK version reported in telemetry and the `x-retrace-client` header. A constant rather than a
 * package.json import so the bundle stays runtime-agnostic (edge runtimes have no fs / JSON modules);
 * version.test.ts fails the build if it ever differs from package.json.
 */
export const SDK_VERSION = "0.16.25";
