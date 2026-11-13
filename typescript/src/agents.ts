/**
 * Multi-agent topology helpers — tag spans with the agent that produced them.
 *
 * Framework-agnostic: run a sub-agent's work inside `withAgent(...)` and every span recorded there
 * (including auto-instrumented LLM calls) inherits the agent id / role / parent, so the trace page
 * can draw the agent topology. Uses AsyncLocalStorage (the same isolation primitive `trace()` uses)
 * so concurrent agents in one process never clobber each other's context. Nested `withAgent` calls
 * set `parentAgentId` automatically — nesting expresses the delegation graph.
 *
 * Framework mapping (explicit — deep auto-detection would couple the SDK to each framework):
 *   - LangGraph: use the node name as the agent id.
 *   - CrewAI: use the agent's role as id + role.
 */
import { AsyncLocalStorage } from "async_hooks";

export interface AgentContext {
  agentId: string;
  agentRole?: string;
  parentAgentId?: string;
  /** Same as `agentId`, matching `withAgent({ id, role, parent })`. */
  id: string;
  /** Same as `agentRole`. */
  role?: string;
  /** Same as `parentAgentId`. */
  parent?: string;
}

/** What `withAgent` takes: `{ id, role, parent }`, or the `AgentContext` field names. */
export type AgentSpec =
  | { id: string; role?: string; parent?: string }
  | { agentId: string; agentRole?: string; parentAgentId?: string };

const agentStore = new AsyncLocalStorage<AgentContext>();

/** The agent context spans are currently tagged with, or undefined outside any `withAgent` scope. */
export function currentAgent(): AgentContext | undefined {
  return agentStore.getStore();
}

/**
 * Scope every span created inside `fn` to `agent.id`. `parent` defaults to the enclosing
 * `withAgent` scope's id (the handoff edge).
 */
export function withAgent<T>(agent: AgentSpec, fn: () => T): T {
  const prev = agentStore.getStore();
  const id = "agentId" in agent ? agent.agentId : agent.id;
  const role = "agentId" in agent ? agent.agentRole : agent.role;
  const parent = ("agentId" in agent ? agent.parentAgentId : agent.parent) ?? prev?.agentId;
  const ctx: AgentContext = { agentId: id, agentRole: role, parentAgentId: parent, id, role, parent };
  return agentStore.run(ctx, fn);
}
