# Design notes

This follows the shape of Raft as described in the original paper (Ongaro &
Ousterhout), scoped down to what the project actually asks for: leader
election, log replication, and a state machine that survives node
failures and network partitions.

## Why TCP with newline-delimited JSON instead of something fancier

I didn't want to pull in gRPC or an HTTP framework for a project this size,
and Raft messages are small and simple (a handful of fields). Each RPC
opens a fresh connection, writes one JSON line, waits for one JSON line
back, and closes. It's not efficient - a real system would keep persistent
connections open - but it keeps the network code short enough to actually
read in one sitting, and it made it trivial to simulate packet loss and
partitions (just refuse to open the connection, or drop the request before
sending it).

## Leader election

Each node starts as a follower with a randomized election timeout (800ms
- 1500ms). If it doesn't hear from a leader in that window, it becomes a
candidate, bumps its term, votes for itself, and asks everyone else for a
vote. The randomization is the whole trick to avoiding split votes - if
every node timed out at the same instant they'd all become candidates at
once and nobody would get a majority.

One bug I hit early on: I was checking `state !== 'candidate'` in the
timeout handler but not re-checking the term after the vote replies came
back asynchronously. If a node lost an election and started a *new* one
before the old RPCs replied, the old replies would still increment the
`votes` counter for the wrong term. Fixed it by capturing the term at the
start of the election (`electionTerm`) and only counting a vote if the
node's current term still matches it.

## Log replication

The leader tracks `nextIndex` per follower (what entry to send next) and
`matchIndex` (highest entry we know is replicated there). AppendEntries
carries a `prevLogIndex`/`prevLogTerm` pair so the follower can check its
log actually matches the leader's up to that point before accepting new
entries. If it doesn't match, the follower reports back a conflict index
and term, and the leader backs `nextIndex` up to that point rather than
just decrementing by one each round - this is the optimization from the
paper's extended version, otherwise catching up a follower that's badly
behind takes one round trip per missing entry.

A commit only counts once a majority has the entry *and* the entry is from
the leader's current term. I originally didn't have that second check and
it let a leader commit an old entry just because a majority happened to
have it, even in a case where that entry could theoretically still get
overwritten by a future leader with a different history - the paper calls
this out specifically (figure 8 in the original paper) and it took a while
to convince myself why the term check actually matters versus just index
count.

## Handling crashes and partitions

`crash()` flips an `active` flag and stops the node's timers - it stays in
memory but ignores incoming RPCs and stops sending any. `network.js` has
`blockPeer`/`unblockPeer` to simulate a one-way or two-way partition
without actually killing the process, which is how the test suite verifies
a partitioned minority can't commit writes and a healed partition catches
back up.

## Persistence

This was the main thing missing from my first attempt at this project.
`currentTerm`, `votedFor`, and the log all get written to a JSON file per
node under `.raft-data/` any time they change - a vote granted, a new
election started, or new entries appended. Writes are synchronous
(`writeFileSync`) and go through a temp-file-then-rename so a process that
dies mid-write never leaves a corrupt file behind for the next startup to
choke on.

This is the part of Raft's safety guarantees that's easy to gloss over: if
a node votes for someone and then crashes before persisting that, it could
come back up and vote for a different candidate in the same term, which
is exactly the kind of thing that lets two leaders exist in the same term.
Same idea for accepted log entries - a follower can't tell a leader "yes,
got it" and then lose that entry on restart.

The tradeoff is write latency: every vote and every log append does a
blocking disk write, which caps how fast this can go under high write
load. Batching multiple appends into a single write before acknowledging
would fix that, but it adds complexity I didn't think was worth it for a
project of this scope - I noted it in the README under "what's not here"
territory since it'd matter a lot for a production system.

## What I'd add with more time

- Log compaction/snapshots, since the in-memory log just grows forever
  right now.
- Batching AppendEntries so a slow follower catches up in fewer round
  trips instead of one heartbeat interval at a time.
- A real client-facing API (the CLI is fine for demoing but isn't how a
  service like this would actually get used).
