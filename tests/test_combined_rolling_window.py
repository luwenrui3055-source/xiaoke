"""The client and Timeline share one record-count rolling window."""
from timeline_context import TimelineRecord, combined_window_records


def record(n: int, role: str | None = None) -> TimelineRecord:
    return TimelineRecord(id=f"r{n}", sequence=n, role=role or ("user" if n % 2 else "assistant"), content=str(n))

# User's combined-window example, N=3:
# client 4 -> Timeline 2,3 -> final 2,3,4
records = [record(n) for n in range(1, 5)]
assert [r.sequence for r in combined_window_records([{'role': 'assistant', 'content': '4'}], records, max_records=3)] == [2, 3]

# client 4,5 -> Timeline 3 -> final 3,4,5
records = [record(n) for n in range(1, 6)]
assert [r.sequence for r in combined_window_records([{'role': 'assistant', 'content': '4'}, {'role': 'user', 'content': '5'}], records, max_records=3)] == [3]

# Client already fills N: no extra Timeline records.
full = [{'role': 'assistant', 'content': '4'}, {'role': 'user', 'content': '5'}, {'role': 'assistant', 'content': '6'}]
assert combined_window_records(full, [record(n) for n in range(1, 7)], max_records=3) == []

# Tool material does not consume a client conversation slot. Timeline events do
# consume a selected shared-window slot and are therefore forwarded.
with_tool = [{'role': 'assistant', 'content': '4'}, {'role': 'assistant', 'content': None, 'tool_calls': [{'id': 'call'}]}, {'role': 'tool', 'content': 'result', 'tool_call_id': 'call'}]
assert [r.sequence for r in combined_window_records(with_tool, [record(1), record(2, 'event'), record(3)], max_records=3)] == [2, 3]
print('combined-rolling-window-234-345-ok')
