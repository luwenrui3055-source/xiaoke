# Continuity and window identity

## The rule for shared continuity

Clients share a Timeline only when they all send requests to the **same xiaoke deployment**: the same gateway base URL, the same gateway API key, and therefore the same SQLite Timeline database. Source labels do not create separate Timelines and do not require separate keys.

A source label is optional metadata. If operators want reliable source names in the Timeline, a reverse proxy can add `X-Xiaoke-Source` independently for each client path. This label is useful for display and diagnostics, but it is not the basis of cross-client continuity.

## Rolling context is not a one-time import

When xiaoke detects a new window or a missing shared-history anchor, it builds one **combined rolling window**. `MAX_HANDOFF_RECORDS` is the total record limit for the ordinary `user`/`assistant` conversation already supplied by the client plus eligible records selected from the Timeline. It is not “up to N more records injected after whatever the client already sent.” `MAX_HANDOFF_CHARS` is a secondary character cap for the Timeline portion.

xiaoke counts the client conversation first, then fills only the remaining slots with newest eligible Timeline records absent from that request. Explicit Timeline events are eligible records: they are forwarded to the model and consume a remaining slot. System messages, tool calls, and tool results do not consume this shared record limit.

For a limit of 3, if the Timeline is `1 2 3 4 5`: client `4` receives `2 3` and produces `2 3 4`; client `4 5` receives only `3` and produces `3 4 5`; client `4 5 6` is already full and receives no Timeline records.

These bounds affect only what is sent on a particular request. They never erase older Timeline records. On later turns, xiaoke selects from the latest Timeline again and retains the identified window's continuity baseline. This prevents the common failure where a new window remembers the first handoff reply but loses the shared background on its next reply.

## Window identification

A frontend should send a stable per-conversation identifier whenever possible.

- `X-Conversation-ID` is used when a client provides it.
- `X-Xiaoke-Session-ID` can be supplied by a proxy or client as an explicit stable session key.
- Without a stable identifier, xiaoke uses a conservative history-based fallback. It can recognize some continuations, but it cannot be perfectly reliable when multiple windows have similar or repeated messages.

For reliable behavior, integrate a stable window ID in the client or at its reverse proxy. Do not depend on source labels alone to identify a conversation window.

## What is recorded

xiaoke records successful eligible user and assistant text turns, plus explicit application events. A completed user/assistant turn is committed together. An interrupted upstream stream, failed request, empty turn, system prompt, credential, request header, raw attachment, or incomplete tool-call chain is not eligible for normal Timeline storage or handoff.

System prompts and personas remain owned by each frontend. xiaoke does not turn the Timeline into a replacement system prompt; selected Timeline records remain role-preserving chat messages.
