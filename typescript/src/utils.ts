// Runtime-agnostic UUID + UTF-8 byte helpers. Web Crypto (globalThis.crypto) and TextEncoder/
// TextDecoder are available on Node 20+, Bun, Deno, browsers, and every edge runtime — so we avoid
// a hard `node:crypto` import and the Node-only `Buffer`, both of which break module evaluation on
// edge runtimes (Cloudflare Workers, Vercel Edge, Supabase Edge).
export function genId(): string {
  return globalThis.crypto.randomUUID();
}

const _textEncoder = new TextEncoder();

export function nowIso(): string {
  return new Date().toISOString();
}

export function utcNow(): Date {
  return new Date();
}

/**
 * Deterministic sampling decision using FNV-1a hash.
 * When a seed is provided, the same (seed + key) always produces the same decision.
 * Without a seed, falls back to Math.random() for backward compatibility.
 */
export function shouldSample(rate: number, seed?: string, key?: string): boolean {
  if (rate >= 1) return true;
  if (rate <= 0) return false;
  if (!seed) return Math.random() < rate;
  // FNV-1a hash of seed+key → deterministic float in [0,1)
  const input = `${seed}:${key || Date.now()}`;
  let hash = 2166136261;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return ((hash >>> 0) / 4294967296) < rate;
}

/** Truncate a string to at most `maxBytes` UTF-8 bytes WITHOUT splitting a multi-byte codepoint, so
 *  there's no garbled/replacement-char tail. Iterates whole code points (via the string iterator). */
function sliceStringToBytes(s: string, maxBytes: number): string {
  if (maxBytes <= 0) return "";
  if (_textEncoder.encode(s).length <= maxBytes) return s;
  let out = "";
  let n = 0;
  for (const ch of s) {
    const b = _textEncoder.encode(ch).length;
    if (n + b > maxBytes) break;
    out += ch;
    n += b;
  }
  return out;
}

export function truncateJson(obj: unknown, maxBytes = 10240): unknown {
  let s: string | undefined;
  try { s = JSON.stringify(obj); } catch { s = undefined; }
  // Not JSON-serializable (circular ref, BigInt, …): char-safe string slice, not a broken parse.
  if (typeof s !== "string") return sliceStringToBytes(String(obj), maxBytes);
  const byteLen = _textEncoder.encode(s).length;
  if (byteLen <= maxBytes) return obj;
  // Over the limit. Return a self-describing, VALID marker with a UTF-8-safe preview. The old code
  // byte-sliced the JSON mid-codepoint and JSON.parse()'d the fragment — which almost always threw,
  // falling back to String(obj) = "[object Object]" for any object (total data loss) and a garbled
  // tail for strings. `wasTruncated` still flags this so the server refuses to byte-replay it.
  return {
    _truncated: true,
    _original_bytes: byteLen,
    _preview: sliceStringToBytes(s, Math.max(0, maxBytes - 64)),
  };
}

/** True if truncateJson(obj, maxBytes) would drop bytes. Used to flag a span's output as truncated
 *  so the server refuses to byte-replay it (the replayed value would differ from the original). */
export function wasTruncated(obj: unknown, maxBytes = 10240): boolean {
  try {
    return _textEncoder.encode(JSON.stringify(obj)).length > maxBytes;
  } catch {
    return String(obj).length > maxBytes;
  }
}

/** Default per-span-type truncation limits (bytes). */
const DEFAULT_TRUNCATION_LIMITS: Record<string, number> = {
  llm_call: 51200,    // 50KB — LLM prompts can be large
  tool_call: 10240,   // 10KB
  tool_result: 10240, // 10KB
  reasoning: 20480,   // 20KB
  action: 5120,       // 5KB
  error: 5120,        // 5KB
};

let customTruncationLimits: Record<string, number> = {};

/** Configure per-span-type truncation limits. */
export function setTruncationLimits(limits: Record<string, number>): void {
  customTruncationLimits = limits;
}

/** Get the truncation limit for a given span type. */
export function getTruncationLimit(spanType: string): number {
  return customTruncationLimits[spanType] || DEFAULT_TRUNCATION_LIMITS[spanType] || 10240;
}

/** Truncate payload based on span type. */
export function truncateForSpanType(obj: unknown, spanType: string): unknown {
  return truncateJson(obj, getTruncationLimit(spanType));
}

export const MAX_ERROR_CHARS = 2000;

/** The failure a run ended with, as one line: "Error: boom" (no stack — frames carry file paths and
 *  values). undefined when nothing was thrown. */
export function describeError(err: unknown): string | undefined {
  if (err === undefined || err === null) return undefined;
  if (err instanceof Error) return (err.message ? `${err.name}: ${err.message}` : err.name).slice(0, MAX_ERROR_CHARS);
  const text = typeof err === "string" ? err : (() => { try { return JSON.stringify(err); } catch { return String(err); } })();
  return (text || String(err)).slice(0, MAX_ERROR_CHARS);
}

