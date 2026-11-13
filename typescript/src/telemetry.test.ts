import { afterEach, describe, expect, it, vi } from "vitest";
import { configure, telemetryFromEnv } from "./config.js";
import { track } from "./telemetry.js";

describe("telemetry opt-out (pass 7: it could not be turned off)", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    configure({ telemetry: false });
  });

  it("sends nothing when turned off, and one event when on", () => {
    const fetchMock = vi.fn(() => Promise.resolve(new Response(null, { status: 204 })));
    vi.stubGlobal("fetch", fetchMock);
    configure({ telemetry: false });
    track("probe");
    expect(fetchMock).not.toHaveBeenCalled();
    configure({ telemetry: true });
    track("probe");
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(String((fetchMock.mock.calls[0] as unknown[])[0])).toMatch(/\/api\/v1\/internal\/logs$/);
  });

  it("reads RETRACE_TELEMETRY and DO_NOT_TRACK", () => {
    expect(telemetryFromEnv({})).toBe(true);
    for (const v of ["0", "false", "OFF", " no "]) expect(telemetryFromEnv({ RETRACE_TELEMETRY: v })).toBe(false);
    expect(telemetryFromEnv({ RETRACE_TELEMETRY: "1" })).toBe(true);
    expect(telemetryFromEnv({ DO_NOT_TRACK: "1" })).toBe(false);
    expect(telemetryFromEnv({ DO_NOT_TRACK: "0" })).toBe(true);
  });
});
