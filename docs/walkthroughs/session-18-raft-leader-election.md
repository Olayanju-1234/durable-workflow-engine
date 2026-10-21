# Session 18: Raft, part 1. Electing a leader

*Plain-English walkthrough for a Node/TS developer who is new to Go and new to
distributed consensus. This is the first of four sessions on Phase 4. Today's
goal is small on paper: three copies of workflowd that agree, at every moment,
on which one of them is in charge.*

## Why we need this at all

Right now workflowd is one process on one VPS. Its history files are durable
(they survive a crash and a restart), but they are not *available*: if that
machine dies, every workflow stops until someone brings it back.

The fix is to run several copies (nodes) and keep the same history on all of
them. Then any one node can die and another carries on. The hard part is not
copying data. The hard part is *agreeing*. If two nodes both think they are in
charge and both accept writes, you get two different histories for the same
workflow, and there is no way to merge them. That is called **split brain**, and
avoiding it is the whole job of a consensus algorithm.

We use **Raft** (Ongaro and Ousterhout, 2014), which was designed to be the
understandable one. Phase 4 builds it in four steps:

1. Session 18 (this one): leader election.
2. Session 19: log replication, so the leader's writes reach the followers.
3. Session 20: proving the safety rules hold, plus snapshots.
4. Session 21: putting the workflow engine on top.

## The one idea that makes it work: majorities

A cluster of 3 nodes needs 2 to agree on anything. A cluster of 5 needs 3. The
reason is simple arithmetic: **any two majorities overlap in at least one
node.** If a leader needs a majority of votes, two leaders can't be elected in
the same round, because the shared voter would have had to vote twice. If a
write needs a majority of copies before it counts, any future leader (elected
by a majority) talks to at least one node that has that write.

That's also why clusters have odd sizes. 4 nodes need 3 to agree and survive
only 1 failure, the same as 3 nodes. The 4th node buys you nothing.

## Roles and terms

Every node is in one of three roles:

- **Follower**: the default. Does what the leader says.
- **Candidate**: a follower that stopped hearing from a leader and is asking
  for votes.
- **Leader**: won a majority of votes. Sends heartbeats so everyone knows it's
  alive.

Time is divided into **terms**, numbered 1, 2, 3 and so on. Each term starts
with an election and has at most one leader. Every message carries the
sender's term, and there is one rule that every node applies to every message:

> If you see a higher term than yours, your term is over. Adopt the higher term
> and become a follower, whatever you were doing.

Node analogy: think of the term as an optimistic-locking version number (the
`__v` field Mongoose puts on documents). A leader from term 3 that wakes up
after a long GC pause and tries to boss people around gets told "we're on term
5 now", and it immediately demotes itself. A stale leader is harmless because
its version number is stale. People call this a **fencing token**.

## Election timeouts are a debounced function

How does a follower decide the leader is dead? It keeps an **election timer**.
Every heartbeat from the leader resets it. If the timer ever runs out, the
follower assumes the leader is gone and starts an election.

If you've used lodash's `debounce`, that's exactly it:

```ts
const startElection = debounce(campaign, electionTimeout);
onHeartbeat(() => startElection()); // every heartbeat pushes the deadline back
```

As long as heartbeats keep coming faster than the timeout, `campaign` never
runs. When they stop, it fires once.

The timeout is **randomized**: each node picks a value between 1x and 2x the
base timeout (1 to 2 seconds in production). Without that, all followers would
time out at the same instant, all vote for themselves, and nobody would get a
majority (a **split vote**), forever. With random timeouts one node usually
wakes up first, asks for votes, and wins before the others even notice. Same
trick as people in a meeting who all start talking at once, then each waits a
random moment before trying again.

## Asking for votes

A candidate bumps its term, votes for itself, and sends `RequestVote` to
everyone. A voter grants its vote only if:

1. **It hasn't voted for someone else in this term.** One vote per term,
   first come first served.
2. **The candidate's log is at least as up to date as its own.** Compare the
   term of the last entry first, then the length. This doesn't matter yet (the
   logs are empty until Session 19), but it is the rule that later guarantees a
   new leader never misses a committed write. The test
   `TestVoteRequiresUpToDateLog` pins it down already.

Rule 1 only works if the vote **survives a crash**. Picture this: node C votes
for A in term 1, crashes, restarts, has forgotten, and votes for B in term 1.
Now A and B both have a majority. Two leaders, one term. So before a node
answers any message, it writes its term and vote to disk with fsync. That's the
`hardstate` file, written atomically (temp file, fsync, rename, fsync the
directory, the same discipline as the history files).

Node analogy: it's the difference between `res.send(200)` and then saving to
the database, versus `await db.save()` and then `res.send(200)`. If the process
dies in between, the first version told the client something that's now false.

## The code: a reducer plus a side-effect runner

This is the design choice most worth understanding, and it maps to something
you already know.

The algorithm lives in `internal/raft/core.go` as a **pure state machine**. It
never reads the clock, never touches the disk, never opens a socket. It has
three inputs:

```go
c.tick()        // one unit of time passed
c.step(msg)     // a message arrived from a peer
// (Session 19 adds c.propose(data))
```

and one output, a `Ready` batch: "here's the state you must persist, and here
are the messages you must send".

If that sounds like Redux, it is. `core` is the reducer: `(state, action) =>
(newState, effects)`. And `Node` in `node.go` is the middleware (think
redux-saga) that runs the effects in a careful order:

```go
func (n *Node) flush() error {
    rd := n.core.ready()
    if rd.HardState != nil {
        n.storage.saveHardState(rd.HardState) // 1. make it durable
    }
    for _, m := range rd.Messages {
        n.transport.Send(m)                  // 2. only then tell anyone
    }
    n.core.advance(rd)                       // 3. acknowledge
}
```

The `Node` runs one goroutine with a `select` loop, which is Go's version of an
event loop:

```go
for {
    select {
    case <-ticker.C:        // like setInterval firing
        n.core.tick()
    case m := <-n.recvc:    // like a message on an EventEmitter
        n.core.step(m)
    case <-n.stopc:
        return
    }
    n.flush()
}
```

Because only that goroutine ever touches the core, the core needs no locks at
all. That's the same reason Node code rarely needs mutexes: one thread owns the
state.

## Why the pure core matters: deterministic tests

Distributed systems bugs usually need a very specific order of events: this
message arrives late, that node crashes right after voting, the network splits
just here. With real goroutines and real timers you can't reproduce those
orders reliably.

With a pure core you can. `sim_test.go` builds a fake cluster with **no clock,
no goroutines and no sockets**. The test decides when each node ticks and which
message gets delivered, dropped, duplicated or held back. Randomness comes from
a seeded generator, so seed 7 always produces exactly the same run.

`TestRandomizedElectionSafety` uses that to throw 200 random schedules at 3 and
5-node clusters: random ticks, messages delivered out of order, dropped,
duplicated, nodes crashing and restarting, the network splitting into random
groups and healing. A **safety checker** watches every single step for the
rule "never two leaders in one term". At the end the chaos stops and the test
demands that the cluster settles on one leader (liveness).

## Proving the tests have teeth

A test suite that always passes proves nothing unless you know it *can* fail.
So I broke the code on purpose, five ways, and checked each break was caught
(this is mutation testing, what Stryker does for JS):

| Broken rule | Caught by |
|---|---|
| Skip the up-to-date log check when voting | `TestVoteRequiresUpToDateLog` |
| Leader never steps down without a majority | `TestPartitionedLeaderStepsDown` |
| Followers never ignore votes while a leader is live | `TestLiveLeaderSuppressesVotes` |
| Allow more than one vote per term | 4 tests, including the randomized one |
| Accept messages from stale terms | `TestTermRules` |

There's also one teeth test that lives in the suite permanently:
`TestForgottenVoteElectsTwoLeaders` runs the "C forgets its vote" story from
above twice. With real persistence the checker stays clean. With a simulated
node that forgets its vote on restart, the checker must report two leaders in
term 1, or the test fails.

## Two extra rules from the paper

- **CheckQuorum**: a leader counts which followers answered during each
  election-timeout window. If it can't hear a majority, it steps down. A leader
  stuck on the wrong side of a network split stops pretending, and (from
  Session 21) its clients go looking for the real leader.
- **Vote suppression** (paper section 6): a follower that heard from a live
  leader very recently ignores vote requests, even from a higher term. Without
  it, a node that was cut off (and kept bumping its term while alone) would
  depose a healthy leader the moment it reconnects.

There is one known gap: that reconnecting node still causes one extra election
when the leader's next heartbeat reaches it, because its reply carries the
higher term. Raft's **PreVote** extension fixes that and is on the follow-up
list.

## The network: one-way messages over gRPC

The paper describes `RequestVote` as a function call with a return value. Here
every request *and* every reply is its own message, sent with one gRPC call:

```proto
service RaftService {
  rpc Send(Envelope) returns (SendAck);
}
```

`SendAck` only means "I got it". The actual answer comes back later as a
separate `Send` in the other direction. Think webhooks rather than
request/response: the core never waits on the network, and Raft already copes
with lost, duplicated and reordered messages, so nothing is lost by making
delivery fire-and-forget.

The transport (`transport.go`) gives each peer its own goroutine and a queue of
1024 messages. If a peer is down, its queue fills up and new messages are
dropped, and the leader simply re-sends what's missing on its next heartbeat. A
detail that bit me in the design: gRPC's default reconnect backoff grows to two
minutes, which would leave a restarted node deaf for far longer than an
election timeout. The transport caps it at one second.

## Shared record framing

The `hardstate` file uses the same `[length][CRC32C][payload]` records as the
workflow histories. That framing used to be private to `internal/history`, so I
moved it to a tiny package, `internal/record`, and pointed the history store at
it. The on-disk history format is byte-for-byte unchanged, and every existing
history test passes untouched. Session 19's Raft log will be the third user.

## Try it

```bash
make cluster
```

That builds `workflowd` and starts three nodes (`n1`, `n2`, `n3`) with
`--node-id`, `--peers` and their own data dirs under `data/cluster/`. In
another terminal:

```bash
curl -s localhost:8233/api/health
```

Each node (ports 8233, 8234, 8235) reports its role, term and who it thinks
the leader is. Kill the leader's process and ask again: in my run the other two
agreed on a new leader in term 2 about one second later. Restart the killed
node with the same flags and it reloads its old term from disk, hears the new
leader, and quietly becomes a follower.

In this session cluster mode runs **Raft only**. It doesn't serve the workflow
API yet, and the binary says so at startup. Without `--peers` nothing changed
at all: single-node mode, every existing test and production are exactly as
before.

## Go things that showed up

- **`select` over channels** is the event loop. Each `case` is a different
  event source, and Go picks whichever is ready.
- **`sync.Once`** makes `Start` and `Stop` safe to call twice, like a promise
  you can only resolve once.
- **Interfaces** (`Transport`) are structural, like TypeScript: the gRPC
  transport and the test network satisfy it without declaring anything.
- **Seeded randomness** (`rand.New(rand.NewPCG(seed, ...))`) is what makes the
  simulated runs replayable.
- **`slices.Sort` on the peer list**: Go deliberately randomizes map iteration
  order, so anything that must be deterministic iterates a sorted slice
  instead of a map.

## What's next

Session 19: the log itself. The leader accepts entries, sends them to
followers inside `AppendEntries`, counts an entry as committed once a majority
has it on disk, and repairs followers whose logs went off track.
