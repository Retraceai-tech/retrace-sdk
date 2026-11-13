/**
 * Finds the loaded copies of a provider SDK (openai, @anthropic-ai/sdk, @google/genai) so the
 * interceptors can patch their prototypes.
 *
 * Two gaps this closes (pass 7):
 * - Timing. `import("openai")` settles only after the importing module's synchronous code has run,
 *   so a call made right after `init()` went through the unpatched method and was not recorded.
 *   Where the runtime can `require()` an ES module (Node ≥ 20.19 / 22.12), the ES build is loaded
 *   synchronously instead: the SDK is patched before the app's next line runs.
 * - Two builds. Provider packages ship a CommonJS build and an ES build with separate classes. The
 *   SDK patched the ES build only, so a CommonJS app (`require("openai")`) was never recorded. The
 *   CommonJS build is now patched too: at import for a CommonJS entry point, and on every trace
 *   start once the app has loaded it.
 *
 * Every step is best effort: runtimes without these APIs (older Node, bundlers, edge runtimes) fall
 * back to the async `import()`, which is the previous behavior.
 */

interface NodeRequire {
  (id: string): unknown;
  resolve(id: string): string;
  cache: Record<string, { exports?: unknown } | undefined>;
  main?: unknown;
}

type BuiltinLoader = (id: string) => unknown;

let cachedRequire: NodeRequire | null | undefined;

/** A CommonJS `require` rooted at this file, or null where the runtime has none. */
function nodeRequire(): NodeRequire | null {
  if (cachedRequire !== undefined) return cachedRequire;
  cachedRequire = null;
  try {
    // process.getBuiltinModule keeps `node:module` out of the static import graph, so bundles for
    // runtimes without it still load this file.
    const getBuiltin = (globalThis as { process?: { getBuiltinModule?: BuiltinLoader } }).process?.getBuiltinModule;
    const mod = getBuiltin?.("node:module") as { createRequire?: (from: string) => NodeRequire } | undefined;
    if (mod?.createRequire) cachedRequire = mod.createRequire(import.meta.url);
  } catch {
    cachedRequire = null; // not Node, or a bundle where import.meta.url is not a file URL
  }
  return cachedRequire;
}

function esmBuildPath(specifier: string): string | null {
  try {
    const resolve = (import.meta as { resolve?: (s: string) => string }).resolve;
    const url = resolve?.(specifier);
    if (!url?.startsWith("file:")) return null;
    const fileUrl = (globalThis as { process?: { getBuiltinModule?: BuiltinLoader } }).process?.getBuiltinModule?.("node:url") as { fileURLToPath?: (u: string) => string } | undefined;
    return fileUrl?.fileURLToPath?.(url) ?? null;
  } catch {
    return null; // the provider is not installed, or import.meta.resolve is unavailable
  }
}

const commonJsPaths = new Map<string, string | null>();

function resolveOrNull(req: NodeRequire, specifier: string): string | null {
  try {
    return req.resolve(specifier);
  } catch {
    return null; // the provider is not installed
  }
}

function commonJsBuildPath(req: NodeRequire, specifier: string): string | null {
  if (!commonJsPaths.has(specifier)) commonJsPaths.set(specifier, resolveOrNull(req, specifier));
  return commonJsPaths.get(specifier) ?? null;
}

/** Loads a module, or returns undefined where this runtime cannot (e.g. `require()` of an ES module before Node 20.19). */
function tryRequire(req: NodeRequire, path: string): unknown {
  try {
    return req(path);
  } catch {
    return undefined; // the async import() path covers this runtime
  }
}

/**
 * The copies of `specifier` that can be patched right now, without waiting:
 * - when `eager`, the ES build, loaded with `require()` where the runtime supports it;
 * - the CommonJS build if the app has loaded it, or (when `eager`) if the entry point is CommonJS
 *   and the app will therefore load that build.
 */
export function providerModulesNow(specifier: string, eager: boolean): unknown[] {
  const req = nodeRequire();
  if (!req) return [];
  const found: unknown[] = [];
  const esm = eager ? esmBuildPath(specifier) : null;
  const esmModule = esm ? tryRequire(req, esm) : undefined;
  if (esmModule) found.push(esmModule);
  const cjs = commonJsBuildPath(req, specifier);
  if (cjs && cjs !== esm) {
    const cjsModule = req.cache[cjs]?.exports ?? (eager && req.main !== undefined ? tryRequire(req, cjs) : undefined);
    if (cjsModule) found.push(cjsModule);
  }
  return found;
}

type Method = (...args: unknown[]) => unknown;
export type PatchablePrototype = Record<string, unknown>;

/** Reads `root.k1.k2…` from an untyped module without throwing. */
export function pathOf(root: unknown, ...keys: string[]): unknown {
  let v = root;
  for (const k of keys) {
    if (v === null || (typeof v !== "object" && typeof v !== "function")) return undefined;
    v = (v as Record<string, unknown>)[k];
  }
  return v;
}

export interface ProviderPatcher {
  /** Patches every copy loadable now; the returned promise settles once the async import has too. Never rejects. */
  install(): Promise<void>;
  /** Restores every patched prototype. */
  uninstall(): void;
}

/**
 * Patches `methods` on the prototype that `find` locates in each loaded copy of a provider package,
 * at most once per prototype (so repeated installs never double-wrap and double-record).
 * `load` must be a literal `() => import("pkg")` so bundlers can still see the dependency.
 */
export function createProviderPatcher(opts: {
  specifier: string;
  load: () => Promise<unknown>;
  find: (mod: unknown) => PatchablePrototype | null;
  methods: readonly string[];
  wrap: (method: string, original: Method) => Method;
}): ProviderPatcher {
  const patched = new Map<PatchablePrototype, Map<string, Method>>();
  let imported: Promise<void> | null = null;
  const patch = (mod: unknown): void => {
    const proto = opts.find(mod);
    if (!proto || patched.has(proto)) return;
    const originals = new Map<string, Method>();
    for (const name of opts.methods) {
      const original = proto[name];
      if (typeof original !== "function") continue;
      originals.set(name, original as Method);
      proto[name] = opts.wrap(name, original as Method);
    }
    if (originals.size > 0) patched.set(proto, originals);
  };
  return {
    install() {
      for (const mod of providerModulesNow(opts.specifier, imported === null)) patch(mod);
      // A failed import means the optional peer dependency is not installed: nothing to patch.
      imported ??= opts.load().then(patch, (): void => undefined);
      return imported;
    },
    uninstall() {
      for (const [proto, originals] of patched) for (const [name, original] of originals) proto[name] = original;
      patched.clear();
      imported = null;
    },
  };
}
