/**
 * Full cascade replay for Retrace TypeScript SDK.
 *
 * When trace({ resumable: true }) is used, the SDK:
 * 1. Stores the function reference
 * 2. Listens for 'resume' commands on WebSocket
 * 3. Re-executes the function with modified input
 * 4. Streams new spans back
 */

export interface ResumeCommand {
  forkId: string;
  traceId: string;
  traceName: string;
  forkPointSpanId: string;
  /** 0-based ordinal of the fork-point span among the original ordered spans. Pre-fork spans
   *  (counter <= index) are suppressed on re-exec; the server already has them. */
  forkPointIndex?: number;
  modifiedInput: unknown;
  originalArgs?: unknown[];
  /** Server replay-lease generation token; echoed back on the result trace so the server can reject
   *  a superseded (stale) generation and release the correct lease. */
  executionId?: string;
}

// Registry of resumable functions
const resumableFunctions = new Map<string, (...args: unknown[]) => unknown>();

import { getConfig } from "./config.js";
import type { TraceRecorder as TraceRecorderType } from "./recorder.js";

export function registerResumable(name: string, fn: (...args: unknown[]) => unknown): void {
  resumableFunctions.set(name, fn);
}

export function getResumable(name: string): ((...args: unknown[]) => unknown) | undefined {
  return resumableFunctions.get(name);
}

/**
 * Best-effort execution-start / heartbeat claim. Returns false ONLY on a definitive 409 (a newer
 * replay superseded this one). ANY other outcome — network/timeout, 401/403, deploy-window 404, 429,
 * 5xx — returns true and PROCEEDS: this fence is best-effort by design; the server's completion CAS
 * is the final authority that rejects a stale result. It does NOT, on its own, prevent stale work on
 * a claim-endpoint failure.
 */
async function claimExecution(forkId: string, executionId: string): Promise<boolean> {
  try {
    const cfg = getConfig();
    const resp = await fetch(`${cfg.baseUrl}/api/v1/forks/${forkId}/replay/claim`, {
      method: "POST",
      headers: { "x-retrace-key": cfg.apiKey, "Content-Type": "application/json" },
      body: JSON.stringify({ execution_id: executionId }),
      signal: AbortSignal.timeout(5000),
      redirect: "error",
    });
    return resp.status !== 409;
  } catch {
    return true; // ambiguous failure → proceed (best-effort)
  }
}

export async function handleResume(command: ResumeCommand): Promise<boolean> {
  const fn = getResumable(command.traceName);
  if (!fn) return false;

  // Execution-start fence BEFORE creating the recorder, so a superseded replay leaves NO leaked
  // recorder/trace/interceptors/context (the recorder is only started once we're cleared to run).
  if (command.executionId && !(await claimExecution(command.forkId, command.executionId))) {
    console.warn("[retrace] Cascade replay superseded by a newer replay — aborting before execution.");
    return false;
  }

  // Keep-alive heartbeat: refresh our generation while a (possibly long) replay runs so a new dispatch
  // can't reclaim it as stale — which is what would otherwise let a second replay run concurrently.
  // Interval is well under the server lease window; stopped in finally.
  let heartbeat: ReturnType<typeof setInterval> | null = null;
  if (command.executionId) {
    heartbeat = setInterval(() => { void claimExecution(command.forkId, command.executionId!); }, 5 * 60 * 1000);
    if (typeof heartbeat.unref === "function") heartbeat.unref();
  }

  let recorder: TraceRecorderType | null = null;
  let markFailed: ((err: unknown) => void) | null = null;
  try {
    const { TraceRecorder } = await import("./recorder.js");
    const { TraceStatus } = await import("./trace.js");

    recorder = new TraceRecorder({
      name: `Fork: ${command.traceName}`,
      input: command.modifiedInput,
      metadata: {
        _fork_id: command.forkId,
        _fork_of: command.traceId,
        _fork_point: command.forkPointSpanId,
        _fork_execution_id: command.executionId,
        _cascade_replay: true,
      },
      forkPointSpanId: command.forkPointSpanId,
      forkPointIndex: command.forkPointIndex,
    });
    // Finalizer captured with the right enum type — used to end the trace as FAILED if user code
    // throws, so a thrown replay never leaks a running trace / interceptor routing / trace context /
    // an elevated active-recorder count.
    markFailed = (err) => recorder!.end(undefined, TraceStatus.FAILED, { error: err });
    recorder.start(`Fork: ${command.traceName}`, command.modifiedInput);

    // Determine args for re-execution
    let args: unknown[] = command.originalArgs || [];
    if (typeof command.modifiedInput === "string") {
      args = [command.modifiedInput, ...args.slice(1)];
    } else if (typeof command.modifiedInput === "object" && !Array.isArray(command.modifiedInput)) {
      args = [command.modifiedInput];
    }

    const result = await Promise.resolve(fn(...args));
    recorder.end(result, TraceStatus.COMPLETED);
    return true;
  } catch (err) {
    console.error("[retrace] Cascade replay failed:", err);
    try { markFailed?.(err); } catch { /* best-effort finalize */ }
    return false;
  } finally {
    if (heartbeat) clearInterval(heartbeat);
  }
}

export function parseResumeMessage(msg: { type: string; data?: Record<string, unknown> }): ResumeCommand | null {
  if (msg.type !== "resume" || !msg.data) return null;
  return {
    forkId: msg.data.forkId as string,
    traceId: msg.data.traceId as string,
    traceName: msg.data.traceName as string,
    forkPointSpanId: msg.data.forkPointSpanId as string,
    forkPointIndex: msg.data.forkPointIndex as number | undefined,
    modifiedInput: msg.data.modifiedInput,
    originalArgs: msg.data.originalArgs as unknown[],
    executionId: msg.data.executionId as string | undefined,
  };
}
