# Distributed KV Store (Raft)

A small key-value store that stays consistent across 3+ nodes using a
simplified version of the Raft consensus algorithm. Built for the Cultus
distributed systems project.

## Running a cluster

Open three terminals and start one node in each:

```
node index.js 1 localhost:8001 2=localhost:8002 3=localhost:8003
node index.js 2 localhost:8002 1=localhost:8001 3=localhost:8003
node index.js 3 localhost:8003 1=localhost:8001 2=localhost:8002
```

Whichever one wins the election will print "won election". Only that node
will accept writes.

## Commands (type into any node's terminal)

| command | what it does |
|---|---|
| `set key value` | write, only works if this node is the leader |
| `get key` | read from this node's local state |
| `dump` | print the whole kv store on this node |
| `crash` | simulate this node crashing (stops responding to RPCs) |
| `recover` | bring a crashed node back |
| `partition <id>` | cut this node off from peer `<id>` |
| `heal <id>` | undo a partition with peer `<id>` |
| `latency <ms>` | add artificial network delay |
| `loss <rate>` | drop RPCs randomly (0.0 - 1.0) |

## Trying it out

1. Start all 3 nodes, wait a couple seconds for a leader.
2. `set name bhanu` on the leader.
3. `get name` on the other two - they should have it within a second.
4. `crash` on the leader. Watch one of the other two win a new election.
5. `recover` the old leader. It rejoins as a follower and catches up.
6. Restart a node's process entirely (Ctrl+C, then rerun the same command).
   It reloads its term/log from `.raft-data/node-<id>.json` instead of
   starting from zero.

## Testing

```
npm test
```

Runs an actual test suite (`node --test`) that spins up 3 nodes in-process,
kills/partitions/restarts them, and asserts on the resulting state - not a
scripted log of print statements. See `test/raft.test.js`.

## What's not here

- No log compaction / snapshots. The log just grows forever, which is fine
  for a class project but would need fixing for a real long-running system.
- No cluster membership changes (adding/removing nodes at runtime).
- No client-facing HTTP/gRPC API, just the local CLI. Wiring a network API
  on top would be straightforward since `handleSet`/`handleGet` already do
  the real work.

See `DESIGN.md` for the reasoning behind specific decisions and the edge
cases I ran into.
