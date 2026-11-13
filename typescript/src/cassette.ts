/**
 * Golden cassette writer for CI regression replay (Phase 4b).
 *
 * After recording a run, write its cassette to a file you commit to your repo; `retrace ci replay`
 * diffs a fresh run against it in CI. The JSON shape is the cross-language contract in
 * `@retrace/shared` (CassetteSchema) — keep this writer in sync with it.
 */
import type { TraceRecorder } from "./recorder.js";
import { getActiveRecorder } from "./init.js";

// node:fs is loaded LAZILY (not a static import) so this module — re-exported from the package
// barrel — still evaluates on edge runtimes. Golden cassettes are a Node/CI feature (writing a file
// to disk), so on Node we resolve writeFileSync once at module load; on edge writeGoldenCassette
// throws a clear error. The microtask import resolves long before any real post-run cassette write.
let _writeFileSync: ((path: string, data: string) => void) | null = null;
if (typeof process !== "undefined" && process.versions?.node) {
  void import("node:fs").then((m) => { _writeFileSync = m.writeFileSync; }).catch(() => {});
}

export interface CassetteTolerance {
  default?: "exact" | "ignore" | "semantic" | "judge";
  steps?: Record<string, "exact" | "ignore" | "semantic" | "judge">;
  semantic_threshold?: number;
}

/**
 * Write the active (or given) recorder's run to `path` as a golden cassette. Returns the cassette.
 * Throws if no recorder is active — call it inside/after your traced function, before the process
 * exits. `tolerance` is written through for CI divergence budgets.
 */
export function writeGoldenCassette(path: string, opts?: { recorder?: TraceRecorder; tolerance?: CassetteTolerance }): unknown {
  const recorder = opts?.recorder ?? getActiveRecorder();
  if (!recorder) throw new Error("writeGoldenCassette: no active recorder — call inside a traced run");
  const cassette = recorder.toCassette() as Record<string, unknown>;
  if (opts?.tolerance) cassette.tolerance = opts.tolerance;
  if (!_writeFileSync) {
    throw new Error("writeGoldenCassette requires a Node.js filesystem; it isn't available on this runtime (e.g. edge).");
  }
  _writeFileSync(path, JSON.stringify(cassette, null, 2));
  return cassette;
}
