import http from "node:http";
import { createRequire } from "node:module";
import type { AddressInfo } from "node:net";
import OpenAI from "openai";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import type { SpanData } from "../trace.js";
import { installOpenAIInterceptor, uninstallOpenAIInterceptor } from "./openai.js";

// A local stand-in for the OpenAI API: one canned chat completion per request.
let server: http.Server;
let baseURL = "";
beforeAll(async () => {
  server = http.createServer((req, res) => {
    req.resume();
    req.on("end", () => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({
        id: "c1", object: "chat.completion", created: 1, model: "gpt-4o-mini",
        choices: [{ index: 0, message: { role: "assistant", content: "hi" }, finish_reason: "stop" }],
        usage: { prompt_tokens: 3, completion_tokens: 1, total_tokens: 4 },
      }));
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  baseURL = `http://127.0.0.1:${(server.address() as AddressInfo).port}/v1`;
});
afterAll(() => new Promise<void>((resolve) => server.close(() => resolve())));
afterEach(() => uninstallOpenAIInterceptor());

const ask = (client: { chat: { completions: { create: (b: unknown) => Promise<unknown> } } }, content: string) =>
  client.chat.completions.create({ model: "gpt-4o-mini", messages: [{ role: "user", content }] });

const llmInputs = (spans: SpanData[]) =>
  spans.filter((s) => s.span_type === "llm_call").map((s) => (s.input as { messages: Array<{ content: string }> }).messages.at(-1)?.content);

describe("provider patching", () => {
  it("patches synchronously, so a call made right after install is recorded (pass 7: the first call was missed)", async () => {
    const spans: SpanData[] = [];
    void installOpenAIInterceptor((s) => spans.push(s)); // deliberately not awaited
    await ask(new OpenAI({ apiKey: "test", baseURL }), "first");
    expect(llmInputs(spans)).toEqual(["first"]);
  });

  it("patches the CommonJS build an app loads with require() (it was never recorded)", async () => {
    const cjs = createRequire(import.meta.url)("openai") as { OpenAI: typeof OpenAI };
    expect(cjs.OpenAI).not.toBe(OpenAI); // a separate copy with its own classes
    const spans: SpanData[] = [];
    await installOpenAIInterceptor((s) => spans.push(s));
    await ask(new cjs.OpenAI({ apiKey: "test", baseURL }), "from require");
    expect(llmInputs(spans)).toEqual(["from require"]);
  });

  it("wraps each prototype once however often it is installed, and uninstall restores it", async () => {
    const spans: SpanData[] = [];
    const original = OpenAI.Chat.Completions.prototype.create;
    for (let i = 0; i < 3; i++) await installOpenAIInterceptor((s) => spans.push(s));
    await ask(new OpenAI({ apiKey: "test", baseURL }), "once");
    expect(llmInputs(spans)).toEqual(["once"]);
    uninstallOpenAIInterceptor();
    expect(OpenAI.Chat.Completions.prototype.create).toBe(original);
  });
});
