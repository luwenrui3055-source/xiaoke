# Architecture

## Responsibility boundary

xiaoke is a context gateway between chat clients and an OpenAI-compatible upstream. It owns the shared Timeline and the context-handoff decision. Each frontend remains responsible for its own system prompt, persona, memory feature, UI, and local history.

System prompts and persona text are not stored in the Timeline and are not copied between frontends by xiaoke.

## Request lifecycle

1. A client sends an OpenAI-compatible chat-completions request with the xiaoke Bearer key.
2. xiaoke identifies the source as optional metadata and identifies the conversation window when a stable window ID is available.
3. It counts ordinary `user`/`assistant` messages already supplied by the client and examines the history for Timeline overlap.
4. `MAX_HANDOFF_RECORDS` is a combined rolling-window limit: xiaoke selects only enough recent eligible Timeline records not already supplied by the client to fill the remaining slots. It is not an extra injection allowance.
5. Explicit Timeline events are eligible selected records. They are rendered as assistant-context messages for the upstream model and consume a remaining combined-window slot.
6. xiaoke sends the assembled request to the upstream model. Except for rebuilt `messages`, `model`, and `stream`, compatible request options are preserved for forwarding.
6. Only after a complete successful assistant response does xiaoke transactionally store the current eligible user/assistant text turn.

## Storage and ordering

SQLite is the durable store. Records receive an immutable UUID and a monotonic sequence. The sequence is the authoritative ordering field; timestamps are display metadata and may originate from different clients.

A completed user turn and assistant turn are committed together. Failed requests, interrupted streams, and incomplete replies are not committed as handoff-eligible conversation. SQLite WAL mode and transactional writes support concurrent readers with serialized writers.

Timeline records are retained until an operator explicitly deletes them. Leaving the rolling-context budget does not delete a record.

## Handoff model

Handoff records are normal role-preserving messages, not a synthetic replacement system prompt. A frontend's own system messages remain first; selected shared records are placed before that frontend's current messages.

A newly handed-off window receives a server-side continuity baseline. Subsequent requests in that identified window rebuild the current combined rolling window, not a second full history beside the client history. This is necessary because most clients do not return invisible gateway-injected messages in their next API request.

For example, with a limit of 3, client history `4` is filled with Timeline `2, 3` to form `2, 3, 4`; when the next request carries `4, 5`, xiaoke supplies only `3`, producing `3, 4, 5`. Once the client itself supplies 3 ordinary conversation records, xiaoke supplies no additional Timeline records. System messages, tool calls, and tool results do not consume this shared record limit. Selected explicit Timeline events do consume a slot and are forwarded upstream.

See [Continuity and window identity](continuity-and-window-identity.md) for operational requirements and fallback limitations.

## Recording boundary

Eligible records are successful user/assistant text turns and explicit application events. xiaoke excludes system prompts, memory-injection blocks, credentials, request headers, raw attachments, and incomplete tool-call chains from normal Timeline storage and injection.

Tool schemas and compatible tool request options may pass upstream, but tool-call chains are not treated as ordinary shared conversation records.

## Timeline viewer

The optional static viewer reads the same SQLite-backed Timeline through authenticated internal API endpoints. Its public browser routes should be provided by a reverse proxy; the proxy, not browser JavaScript, supplies the gateway Bearer key. See [Timeline viewer deployment](timeline-viewer.md).
