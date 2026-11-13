import { describe, it, expect } from "vitest";
import { recordedMessages, recordedContents, MAX_RECORDED_MESSAGES } from "./messages.js";

// Pass 7: the interceptors kept the FIRST 10 messages (then a 10 KB preview from the start), so the
// newest — the question and the tool results the answer quoted — were dropped and correct answers
// were judged "unsupported".
function conversation(n: number) {
  const msgs: Array<Record<string, unknown>> = [{ role: "system", content: "You are a shipping assistant." }];
  for (let i = 0; i < n; i++) msgs.push({ role: "user", content: `old question ${i}` }, { role: "assistant", content: `old answer ${i}` });
  msgs.push({ role: "user", content: "Where is my order A-1?" }, { role: "tool", tool_call_id: "call_1", content: '{"carrier":"DHL","eta":"2026-10-04"}' });
  return msgs;
}

describe("recordedMessages", () => {
  it("keeps the system message and the newest messages, and says how many were left out", () => {
    const rec = recordedMessages(conversation(6)) as { messages: Array<Record<string, unknown>>; messages_omitted: number };
    expect(rec.messages).toHaveLength(MAX_RECORDED_MESSAGES);
    expect(rec.messages[0]).toEqual({ role: "system", content: "You are a shipping assistant." });
    expect(rec.messages.at(-1)).toEqual({ role: "tool", tool_call_id: "call_1", content: '{"carrier":"DHL","eta":"2026-10-04"}' });
    expect(rec.messages.at(-2)?.content).toBe("Where is my order A-1?");
    expect(rec.messages_omitted).toBe(5);
  });

  it("records a short conversation whole, with extra fields", () => {
    expect(recordedMessages(conversation(1), { response_format: "json" })).toEqual({ messages: conversation(1), response_format: "json" });
  });

  it("shortens long message text (head + tail) instead of dropping the newest messages", () => {
    const msgs = conversation(2).map((m) => ({ ...m, content: `${m.content} ${"z".repeat(3000)} END` }));
    const rec = recordedMessages(msgs) as { messages: Array<{ content: string }> };
    expect(rec.messages).toHaveLength(msgs.length);
    expect(rec.messages.at(-1)!.content.startsWith('{"carrier":"DHL"')).toBe(true);
    expect(rec.messages.at(-1)!.content.endsWith("END")).toBe(true);
    expect(JSON.stringify(rec).length).toBeLessThanOrEqual(10240);
  });
});

describe("recordedContents", () => {
  it("keeps the newest Gemini contents items", () => {
    const items = Array.from({ length: 30 }, (_, i) => `turn ${i}`);
    expect(recordedContents(items)).toEqual(items.slice(20));
    expect(recordedContents("hi")).toBe("hi");
  });
});
