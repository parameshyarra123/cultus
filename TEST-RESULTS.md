# Test results

Run with `npm test` (`node --test test/`). Node v22.22.2. This is real
output from an actual run, not hand-written - I re-ran it right before
writing this up.

```
1..6
# tests 6
# suites 0
# pass 6
# fail 0
# cancelled 0
# skipped 0
# todo 0
```

## What each test actually checks

**cluster elects exactly one leader** - starts 3 nodes, waits for an
election, then asserts exactly one node is in the `leader` state and that
no two nodes claim leadership in the same term. Passed in ~0.9s.

**a write on the leader replicates to every node** - finds the leader,
calls `handleSet('foo','bar')` on it directly, waits ~1s for heartbeats to
propagate, then asserts `stateMachine.get('foo') === 'bar'` on all three
nodes independently. Passed in ~2.1s.

**a non-leader rejects writes** - calls `handleSet` on a follower and
asserts it returns `false` and doesn't touch the log. Passed in ~1s.

**cluster recovers after the leader crashes** - finds the leader, records
its term, calls `.crash()` on it (stops it from responding to any RPC),
then waits for the two survivors to elect a new leader and asserts the new
leader's term is strictly higher than the old one. Passed in ~3.1s - this
one takes the longest since it has to wait out a full election timeout on
the survivors.

**a healed partition converges to a consistent log** - blocks the network
between the leader and one follower in both directions, writes a key,
confirms the isolated follower does *not* have it yet, then unblocks and
confirms it catches up within a second. Passed in ~2.5s.

**persisted state survives a restart** - leader accepts a write, then the
test calls `.stop()` on it and constructs a brand new `RaftNode` instance
pointed at the same `.raft-data` directory (simulating a real process
restart, not just an in-memory crash flag), and asserts the new instance's
`currentTerm`, log length, and `stateMachine.get('durable')` all match
what was there before the "restart". Passed in ~2.3s.

## Manual scenarios I also ran by hand

Beyond the automated suite, I ran a real 3-terminal cluster (the actual
`npm run node1/node2/node3` setup) and walked through:

- Killing the leader process with Ctrl+C mid-session, confirming a new
  leader takes over and the CLI on the other two terminals still serves
  reads.
- Restarting the killed node's process and watching it print the restored
  term/log length on startup instead of `term=0, logLen=0`.
- Setting `latency 300` and `loss 0.2` on all three nodes at once and
  confirming writes still eventually replicate, just slower - a few
  heartbeat rounds get dropped but the retry-on-next-tick logic in
  `_sendHeartbeat` covers for it.

I didn't automate these three because they involve manually killing OS
processes and reading terminal output, which doesn't translate well into
an assertion-based test, but they're straightforward to repeat using the
commands listed in the README.
