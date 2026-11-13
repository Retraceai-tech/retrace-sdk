import { dispatchInterceptedSpan } from "./_dispatch.js";
import { installGeminiInterceptor } from "./gemini.js";
import { installOpenAIInterceptor } from "./openai.js";
import { installAnthropicInterceptor } from "./anthropic.js";

/**
 * Install ALL provider interceptors against the single stable dispatcher. Idempotent and memoized.
 * Called at import time and from configure()/init(). Where the runtime can `require()` an ES module
 * the providers are patched synchronously, before the app's next line runs; elsewhere the patch
 * lands when the async import settles (see ./provider-modules.ts), which `ready()` waits for.
 */
let installPromise: Promise<void> | null = null;
let settled = false;

export function ensureInterceptorsInstalled(): Promise<void> {
  if (installPromise) return installPromise;
  installPromise = Promise.all([
    installGeminiInterceptor(dispatchInterceptedSpan),
    installOpenAIInterceptor(dispatchInterceptedSpan),
    installAnthropicInterceptor(dispatchInterceptedSpan),
  ]).then(() => { settled = true; });
  return installPromise;
}

/** True once every provider patch has landed (the async imports included). */
export function interceptorsSettled(): boolean {
  return settled;
}

/**
 * Resolves once the installed provider SDKs (openai, @anthropic-ai/sdk, @google/genai) are patched.
 * On current Node this is already true when `retrace-sdk` finishes importing; await it before a
 * first LLM call made at the top of a module on runtimes that cannot `require()` ES modules
 * (Node < 20.19, some bundlers). Never rejects.
 */
export function ready(): Promise<void> {
  return ensureInterceptorsInstalled();
}
