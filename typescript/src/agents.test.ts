import { describe, it, expect } from "vitest";
import { withAgent, currentAgent } from "./agents.js";
import { SpanBuilder, SpanType } from "./trace.js";

describe("withAgent context", () => {
  it("sets the agent context inside the scope and clears it after", () => {
    expect(currentAgent()).toBeUndefined();
    withAgent({ id: "planner", role: "Planner" }, () => {
      expect(currentAgent()).toEqual({ agentId: "planner", agentRole: "Planner", parentAgentId: undefined, id: "planner", role: "Planner", parent: undefined });
    });
    expect(currentAgent()).toBeUndefined();
  });

  it("derives parentAgentId from the enclosing scope when nested", () => {
    withAgent({ id: "orchestrator" }, () => {
      withAgent({ id: "worker", role: "Worker" }, () => {
        expect(currentAgent()).toEqual({ agentId: "worker", agentRole: "Worker", parentAgentId: "orchestrator", id: "worker", role: "Worker", parent: "orchestrator" });
      });
    });
  });

  it("an explicit parent overrides the enclosing scope", () => {
    withAgent({ id: "a" }, () => {
      withAgent({ id: "b", parent: "external" }, () => {
        expect(currentAgent()?.parentAgentId).toBe("external");
      });
    });
  });
});

describe("AgentContext names (pass 7: withAgent took { id } but currentAgent() returned { agentId })", () => {
  it("withAgent accepts either naming and currentAgent() exposes both", () => {
    withAgent({ agentId: "critic", agentRole: "Reviewer" }, () => {
      expect(currentAgent()).toMatchObject({ id: "critic", agentId: "critic", role: "Reviewer", agentRole: "Reviewer" });
    });
  });
});

describe("SpanBuilder.setAgent", () => {
  it("writes all three agent fields onto the span payload", () => {
    const sb = new SpanBuilder("step", SpanType.TOOL_CALL).start();
    sb.setAgent({ id: "w", parentId: "o", role: "Worker" });
    const data = sb.end({ ok: true });
    expect(data.agent_id).toBe("w");
    expect(data.parent_agent_id).toBe("o");
    expect(data.agent_role).toBe("Worker");
  });
});
