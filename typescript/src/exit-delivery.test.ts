/**
 * The quickstart as a real short-lived Node process (pass-5 P1). Against a local stand-in for the API
 * that reads frames one at a time and acks each after a delay, the process must EXIT on its own and the
 * server must have every frame — on the `ws` package (unref'd socket) and on Node's global WebSocket
 * (which cannot be unref'd: the process used to hang forever), and on SIGTERM mid-run.
 */
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { spawn, execFileSync } from "node:child_process";
import { createServer, type Server } from "node:http";
import { mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer } from "ws";

const pkgDir = join(dirname(fileURLToPath(import.meta.url)), "..");
// Compiled fresh from src for this run (inside node_modules so `ws` resolves from the package).
const outDir = join(pkgDir, "node_modules", ".cache", "retrace-exit-test");
const hasGlobalWebSocket = typeof (globalThis as { WebSocket?: unknown }).WebSocket === "function";

type Frame = { type: string; seq?: number; data?: Record<string, unknown> };
let server: Server;
let wss: WebSocketServer;
let port = 0;
const frames: Frame[] = [];
// Trace bodies uploaded over HTTP (the transport's fallback before the socket is up).
const posted: Array<Record<string, unknown> & { spans?: unknown[] }> = [];

beforeAll(async () => {
  rmSync(outDir, { recursive: true, force: true });
  const tsc = createRequire(import.meta.url).resolve("typescript/bin/tsc");
  execFileSync(process.execPath, [tsc, "-p", join(pkgDir, "tsconfig.json"), "--outDir", outDir, "--declaration", "false"], { stdio: "pipe" });
  writeFileSync(join(outDir, "package.json"), JSON.stringify({ type: "module" }));
  // Blocks the optional `ws` package so the SDK falls back to Node's global WebSocket.
  writeFileSync(join(outDir, "block-ws-hooks.mjs"), `export async function resolve(s, c, next) {
    if (s === "ws") { const e = new Error("blocked by test"); e.code = "ERR_MODULE_NOT_FOUND"; throw e; }
    return next(s, c);
  }`);
  writeFileSync(join(outDir, "block-ws.mjs"), `import { register } from "node:module"; register("./block-ws-hooks.mjs", import.meta.url);`);
  writeFileSync(join(outDir, "quickstart.mjs"), `
    import { init, shutdown, SpanType, TraceStatus } from "./index.js";
    const rec = init({ name: "broken-quickstart-agent", baseUrl: process.env.K_BASE });
    for (let i = 0; i < 8; i++) {
      const s = rec.startSpan("search", SpanType.TOOL_CALL, { query: "weather in SF" });
      rec.endSpan(s, "(no new information)");
    }
    const bad = rec.startSpan("get_weather", SpanType.TOOL_CALL, { city: 12345, units: true });
    rec.endSpan(bad, undefined, "TypeError: 'city' must be a string");
    if (process.env.K_HANG) { console.log("recorded"); setInterval(() => {}, 1000); }
    else { shutdown("gave up after looping", TraceStatus.FAILED); console.log("Sent a broken run to Retrace."); }
  `);

  server = createServer((req, res) => {
    let body = "";
    req.on("data", (d) => (body += d));
    req.on("end", () => {
      if (req.method === "POST" && req.url === "/api/v1/traces") posted.push(JSON.parse(body));
      res.writeHead(200, { "content-type": "application/json" });
      res.end("{}");
    });
  });
  wss = new WebSocketServer({ server, path: "/ws/v1/stream" });
  wss.on("connection", (sock) => {
    // Serial processing with a delay per frame, like the API's per-connection inbox.
    let chain = Promise.resolve();
    sock.on("message", (raw) => {
      const m = JSON.parse(String(raw)) as Frame;
      chain = chain.then(async () => {
        if (m.type === "auth") { sock.send(JSON.stringify({ type: "auth_ok" })); return; }
        await new Promise((r) => setTimeout(r, 15));
        frames.push(m);
        if (sock.readyState === sock.OPEN) sock.send(JSON.stringify({ type: "ack", id: m.data?.id, seq: m.seq }));
      });
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  port = (server.address() as { port: number }).port;
}, 60_000);

afterAll(async () => {
  wss?.close();
  await new Promise((r) => server?.close(r));
  rmSync(outDir, { recursive: true, force: true });
});

function run(opts: { blockWs?: boolean; hang?: boolean } = {}): Promise<{ code: number | null; ms: number; out: string; err: string; child: ReturnType<typeof spawn> }> {
  const args = [...(opts.blockWs ? ["--import", join(outDir, "block-ws.mjs")] : []), join(outDir, "quickstart.mjs")];
  const env: NodeJS.ProcessEnv = { ...process.env, RETRACE_API_KEY: "rt_test", K_BASE: `http://127.0.0.1:${port}`, RETRACE_TELEMETRY: "0" };
  delete env.RETRACE_TRANSPORT;
  if (opts.hang) env.K_HANG = "1";
  const started = Date.now();
  const child = spawn(process.execPath, args, { env, stdio: ["ignore", "pipe", "pipe"] });
  let out = "", err = "";
  child.stdout!.on("data", (d) => (out += d));
  child.stderr!.on("data", (d) => (err += d));
  const done = new Promise<{ code: number | null; ms: number; out: string; err: string; child: typeof child }>((resolve) => {
    const kill = setTimeout(() => child.kill("SIGKILL"), 20_000); // a hang fails the test instead of the suite
    child.on("exit", (code, signal) => { clearTimeout(kill); resolve({ code: code ?? (signal ? -1 : null), ms: Date.now() - started, out, err, child }); });
  });
  (done as unknown as { child: typeof child }).child = child;
  return done;
}

function framesOf(traceId: string) {
  return frames.filter((f) => (f.data?.trace_id ?? f.data?.id) === traceId);
}
const lastTraceId = () => [...frames].reverse().find((f) => f.type === "trace_started")!.data!.id as string;

describe("CommonJS consumers", () => {
  it("require('retrace-sdk') loads the package (the exports map had only an `import` condition)", () => {
    // An installed copy of this package: its real package.json, with dist/ = the build above.
    const root = join(outDir, "consumer");
    const pkg = join(root, "node_modules", "retrace-sdk");
    mkdirSync(pkg, { recursive: true });
    writeFileSync(join(pkg, "package.json"), readFileSync(join(pkgDir, "package.json")));
    symlinkSync(outDir, join(pkg, "dist"), "dir");
    writeFileSync(join(root, "package.json"), JSON.stringify({ name: "consumer", type: "commonjs" }));
    writeFileSync(join(root, "main.cjs"), `
      const sdk = require("retrace-sdk");
      require("retrace-sdk/adapters/langchain");
      require("retrace-sdk/adapters/vercel-ai");
      console.log(typeof sdk.init, typeof sdk.trace, sdk.SpanType.TOOL_CALL);
    `);
    const out = execFileSync(process.execPath, [join(root, "main.cjs")], { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, RETRACE_TELEMETRY: "0" } });
    expect(out.trim()).toBe("function function tool_call");
  });
});

describe("the quickstart as a short-lived process", () => {
  it("exits by itself and every frame reached the server (ws package)", async () => {
    const r = await run();
    expect(r.code).toBe(0);
    expect(r.ms).toBeLessThan(10_000);
    const mine = framesOf(lastTraceId());
    expect(mine.filter((f) => f.type === "span_started")).toHaveLength(9);
    expect(mine.filter((f) => f.type === "span_ended")).toHaveLength(9);
    expect(mine.find((f) => f.type === "trace_ended")?.data).toMatchObject({ status: "failed", output: "gave up after looping", span_count: 9 });
    expect(r.err).not.toMatch(/INCOMPLETE|not confirmed/);
  });

  it.runIf(hasGlobalWebSocket)("exits by itself on Node's global WebSocket too (it used to hang forever)", async () => {
    const r = await run({ blockWs: true });
    expect(r.code).toBe(0);
    expect(r.ms).toBeLessThan(10_000);
    const mine = framesOf(lastTraceId());
    expect(mine).toHaveLength(20);
    expect(r.err).not.toMatch(/INCOMPLETE|not confirmed/);
  });

  it("delivers the run on SIGTERM, marked terminated early", async () => {
    const before = { frames: frames.length, posted: posted.length };
    const pending = run({ hang: true });
    const child = (pending as unknown as { child: ReturnType<typeof spawn> }).child;
    await new Promise<void>((resolve) => child.stdout!.on("data", (d) => { if (String(d).includes("recorded")) resolve(); }));
    child.kill("SIGTERM");
    const r = await pending;
    expect(r.code).toBe(143);
    // Over the socket if it was up when the signal arrived, else over HTTP — never lost.
    const viaWs = frames.slice(before.frames);
    const viaHttp = posted.slice(before.posted);
    const ended = viaWs.find((f) => f.type === "trace_ended")?.data ?? viaHttp[0];
    const spans = viaWs.length ? viaWs.filter((f) => f.type === "span_started").length : viaHttp[0]?.spans?.length;
    expect(spans).toBe(9);
    expect(ended).toMatchObject({ terminated_early: true, span_count: 9 });
    expect(r.err).not.toMatch(/INCOMPLETE|not confirmed/);
  });
});
