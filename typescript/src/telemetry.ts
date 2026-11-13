/**
 * Anonymous diagnostic telemetry (on by default, can be turned off).
 *
 * Sends NO user code, API keys, prompts, responses, or personal information — only the SDK version,
 * the runtime/OS family, an anonymous per-process id, and event/error *categories*. Events are POSTed
 * to the Retrace API's internal ingest, which forwards them to PostHog (and the logging platform)
 * under service="sdk-typescript" — so the SDK NEVER holds any analytics key (SDK → Retrace API →
 * PostHog).
 *
 * Fire-and-forget: never blocks the user's program, never throws. Anonymous by construction (no PII,
 * random per-process id). Turn it off with `configure({ telemetry: false })`, RETRACE_TELEMETRY=0 or
 * DO_NOT_TRACK=1: nothing is sent at all then.
 */
import os from "node:os";
import { getConfig } from "./config.js";
import { SDK_VERSION } from "./version.js";

// Per-process random id — anonymous, not linked to any account, user, or machine identity.
const ANON_ID = Math.random().toString(16).slice(2, 18);

/** Coarse provider family from a model name (analytics only — never the key or content). */
export function providerOf(model?: string | null): string {
  const m = (model || "").toLowerCase();
  if (!m) return "";
  if (m.startsWith("gemini") || m.includes("google")) return "google";
  if (/^(gpt|o1|o3|o4|chatgpt|text-embedding|davinci)/.test(m)) return "openai";
  if (m.includes("claude")) return "anthropic";
  return "other";
}

export function track(event: string, fields: Record<string, string | number | boolean> = {}): void {
  try {
    if (!getConfig().telemetry) return;
    const base = (getConfig().baseUrl || "https://api.retraceai.tech").replace(/\/$/, "");
    const body = JSON.stringify({
      service: "sdk-typescript",
      events: [
        {
          level: "info",
          message: `sdk-typescript ${event}`,
          event,
          sdk_version: SDK_VERSION,
          node: process.version,
          os: os.platform(),
          anon_id: ANON_ID,
          ...fields,
        },
      ],
    });
    // Fire-and-forget; never block or throw.
    void fetch(`${base}/api/v1/internal/logs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
    }).catch(() => {});
  } catch {
    /* telemetry must never affect the user's program */
  }
}

/** Emit record_success / record_failure with anonymous, aggregate-only fields (span count, provider
 *  families used, whether the run was resumable / a cascade replay). Never throws. */
export function trackRecord(
  status: string,
  spans: Array<{ model?: string | null }>,
  metadata: Record<string, unknown> = {},
): void {
  try {
    const provs = Array.from(new Set(spans.map((s) => providerOf(s.model)).filter(Boolean)));
    track(status === "completed" ? "record_success" : "record_failure", {
      spans: spans.length,
      providers: provs.join(",") || "none",
      resumable: !!metadata._resumable,
      cascade: !!metadata._cascade_replay,
    });
  } catch {
    /* never throw */
  }
}
