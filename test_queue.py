# Self-check for the offline queue: chronological replay, no loss.
import os, tempfile
from agent import OfflineQueue

db = os.path.join(tempfile.mkdtemp(), "q.db")
q = OfflineQueue(db)
for i in range(5):
    q.put("t/status", f"msg-{i}", i)
assert q.depth() == 5

sent = []
q.drain(lambda t, p: (sent.append(p), True)[1])
assert sent == [f"msg-{i}" for i in range(5)], sent  # chronological order
assert q.depth() == 0

# failure mid-drain keeps remaining messages
for i in range(3):
    q.put("t/status", f"m{i}", i)
calls = []
q.drain(lambda t, p: (calls.append(p), len(calls) < 2)[1])  # fail on 2nd
assert q.depth() == 2, q.depth()  # m1 failed -> m1, m2 still queued
print("queue self-check OK")
