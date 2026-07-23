# Complexity notes

Quick rundown of the costs of the main operations, plus what's actually
providing the synchronization here (there's no locks/mutexes in the usual
sense since this is single-threaded Node - the "synchronization primitive"
is really the RPC protocol itself plus the fact that JS callbacks never
run concurrently with each other on one node).

## Election

- Messages: O(N) - the candidate sends one RequestVote to every other
  node, in parallel.
- Time: bounded by the RPC timeout (1s here) plus the random election
  timeout window (800-1500ms), so a full failed-then-retried election is
  a small constant number of these windows, not something that scales
  with N.

## Log replication / heartbeats

- Messages: O(N) per heartbeat interval - the leader sends one
  AppendEntries to every follower every 100ms, whether or not there's new
  data (empty entries array = pure heartbeat).
- Per-message size: O(k) where k is however many new entries that
  follower is missing. In the steady state (follower caught up) this is
  O(1) - just the heartbeat with an empty entries list.
- Catching up a badly behind follower: the conflictIndex/conflictTerm
  backtracking means the leader can jump `nextIndex` back to the last
  place the logs agree in one round trip, rather than walking back one
  entry per RPC. Worst case is still O(log length) round trips if the logs
  diverge at every single entry, but in practice (a follower that just
  missed a few heartbeats) it's O(1).

## Commit index advancement

- O(N) per new log entry - the leader checks, for every uncommitted
  index, whether a majority of `matchIndex` values have reached it. This
  runs after every successful AppendEntries reply, so worst case it's
  O(N * log length) if you got wildly unlucky and every reply moved
  matchIndex forward by a different amount, but with a healthy cluster
  it's O(N) most ticks since only 1-2 indices are usually still pending.

## Persistence

- O(log length) per write, since it serializes the *entire* log to JSON
  every time and writes it out - `writeFileSync` on the whole array, not
  an append. That's the one thing here that doesn't scale well: a log with
  100k entries means every single vote or new entry costs an O(n) disk
  write. Fine for a class project's data volumes, not fine for a real
  system - the standard fix is an append-only log file (write just the new
  entry, O(1)) plus periodic snapshots to bound total size. I called this
  out in DESIGN.md too since it's the same "would need fixing for
  production" theme.

## "Synchronization primitives" in a single-threaded event loop

Node's event loop means only one callback runs at a time on a given node,
so there's no data race on `this.log` or `this.currentTerm` the way there
would be with OS threads. The actual coordination problem here isn't
thread-safety within one node, it's coordinating state *across* nodes,
which is what the term numbers, vote counting, and prevLogIndex/prevLogTerm
consistency checks are doing - they're the distributed equivalent of a
lock: a node won't advance its own state (accept a log entry, count a
vote) until the message it got proves the sender's view of the world is at
least as current as its own.
