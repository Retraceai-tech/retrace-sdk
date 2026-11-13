/**
 * What an LLM span records as its input messages.
 *
 * The interceptors recorded the FIRST 10 messages of a call (then cut the whole thing to a 10 KB
 * preview, from its start). In a longer conversation that dropped the newest messages — the user's
 * latest question and the tool results the model answered from — so the recorded input no longer
 * contained what the answer was grounded in, and the hallucination detector flagged correct answers
 * (pass 7: "0% of claims supported" for an answer copied from the tool result). Now the leading system
 * message (its instructions) plus the NEWEST messages are kept, whole messages first; only when they
 * are still too large is each message's text shortened (head + tail), so the newest messages survive.
 */
import { truncateJson } from "../utils.js";

const encoder = new TextEncoder();
const bytes = (v: unknown) => encoder.encode(JSON.stringify(v)).length;

export const MAX_RECORDED_MESSAGES = 10;
const MAX_INPUT_BYTES = 10240;
const MAX_MESSAGE_CHARS = 1000;

type Msg = Record<string, unknown>;

function role(m: unknown): unknown {
  return m && typeof m === "object" ? (m as Msg).role : undefined;
}

/** A message whose text content is cut to head + tail (structured parts are kept as they are). */
function shortened(m: unknown): unknown {
  if (!m || typeof m !== "object") return m;
  const content = (m as Msg).content;
  if (typeof content !== "string" || content.length <= MAX_MESSAGE_CHARS) return m;
  const head = Math.floor(MAX_MESSAGE_CHARS * 0.6);
  return { ...(m as Msg), content: `${content.slice(0, head)} […] ${content.slice(content.length - (MAX_MESSAGE_CHARS - head - 5))}` };
}

/** `{ messages, messages_omitted? }` for a span's input (extra fields such as response_format kept). */
export function recordedMessages(messages: unknown[], extra: Record<string, unknown> = {}): unknown {
  const all = Array.isArray(messages) ? messages : [];
  let kept = all;
  let omitted = 0;
  if (all.length > MAX_RECORDED_MESSAGES) {
    const head = role(all[0]) === "system" ? [all[0]] : [];
    kept = [...head, ...all.slice(all.length - (MAX_RECORDED_MESSAGES - head.length))];
    omitted = all.length - kept.length;
  }
  const build = (list: unknown[]) => ({ messages: list, ...(omitted ? { messages_omitted: omitted } : {}), ...extra });
  const whole = build(kept);
  if (bytes(whole) <= MAX_INPUT_BYTES) return whole;
  return truncateJson(build(kept.map(shortened)), MAX_INPUT_BYTES);
}

/** Gemini `contents`: the newest items when it is a long list. */
export function recordedContents(contents: unknown): unknown {
  if (Array.isArray(contents) && contents.length > MAX_RECORDED_MESSAGES) {
    return truncateJson(contents.slice(contents.length - MAX_RECORDED_MESSAGES));
  }
  return truncateJson(contents);
}
