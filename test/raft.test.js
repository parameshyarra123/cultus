import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'fs';
import RaftNode from '../src/node.js';
import { sleep } from '../src/utils.js';



let portCounter = 9100;
function nextPortSet() {
  const base = portCounter;
  portCounter += 3;
  return [base, base + 1, base + 2];
}

function makeCluster(ports, dataDir) {
  const peerAddrs = {
    1: `localhost:${ports[0]}`,
    2: `localhost:${ports[1]}`,
    3: `localhost:${ports[2]}`,
  };
  const nodes = [1, 2, 3].map((id) => new RaftNode(id, peerAddrs[id], peerAddrs, { dataDir }));
  return nodes;
}

async function startAll(nodes) {
  await Promise.all(nodes.map((n) => n.start()));
}

function stopAll(nodes) {
  for (const n of nodes) n.stop();
}

async function waitForLeader(nodes, timeoutMs = 4000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const leaders = nodes.filter((n) => n.active && n.state === 'leader');
    if (leaders.length >= 1) return leaders;
    await sleep(50);
  }
  return [];
}

test('cluster elects exactly one leader', async () => {
  const ports = nextPortSet();
  const dataDir = `.test-data-${ports[0]}`;
  const nodes = makeCluster(ports, dataDir);
  try {
    await startAll(nodes);
    const leaders = await waitForLeader(nodes);
    assert.equal(leaders.length, 1, 'exactly one node should become leader');

    
    const leaderTerms = nodes.filter((n) => n.state === 'leader').map((n) => n.currentTerm);
    assert.equal(new Set(leaderTerms).size, leaderTerms.length);
  } finally {
    stopAll(nodes);
    fs.rmSync(dataDir, { recursive: true, force: true });
  }
});

test('a write on the leader replicates to every node', async () => {
  const ports = nextPortSet();
  const dataDir = `.test-data-${ports[0]}`;
  const nodes = makeCluster(ports, dataDir);
  try {
    await startAll(nodes);
    const [leader] = await waitForLeader(nodes);
    assert.ok(leader, 'expected a leader to exist');

    const accepted = leader.handleSet('foo', 'bar');
    assert.equal(accepted, true);

    
    await sleep(1000);

    for (const n of nodes) {
      assert.equal(n.stateMachine.get('foo'), 'bar', `node ${n.id} should have replicated the write`);
    }
  } finally {
    stopAll(nodes);
    fs.rmSync(dataDir, { recursive: true, force: true });
  }
});

test('a non-leader rejects writes', async () => {
  const ports = nextPortSet();
  const dataDir = `.test-data-${ports[0]}`;
  const nodes = makeCluster(ports, dataDir);
  try {
    await startAll(nodes);
    await waitForLeader(nodes);
    const follower = nodes.find((n) => n.state !== 'leader');
    const accepted = follower.handleSet('x', '1');
    assert.equal(accepted, false);
  } finally {
    stopAll(nodes);
    fs.rmSync(dataDir, { recursive: true, force: true });
  }
});

test('cluster recovers after the leader crashes', async () => {
  const ports = nextPortSet();
  const dataDir = `.test-data-${ports[0]}`;
  const nodes = makeCluster(ports, dataDir);
  try {
    await startAll(nodes);
    const [leader] = await waitForLeader(nodes);
    const oldLeaderId = leader.id;
    const oldTerm = leader.currentTerm;

    leader.crash();

    const survivors = nodes.filter((n) => n.id !== oldLeaderId);
    const newLeaders = await waitForLeader(survivors);
    assert.equal(newLeaders.length, 1, 'a new leader should be elected among survivors');
    assert.ok(newLeaders[0].currentTerm > oldTerm, 'new leader should be in a higher term');
  } finally {
    stopAll(nodes);
    fs.rmSync(dataDir, { recursive: true, force: true });
  }
});

test('a healed partition converges to a consistent log', async () => {
  const ports = nextPortSet();
  const dataDir = `.test-data-${ports[0]}`;
  const nodes = makeCluster(ports, dataDir);
  try {
    await startAll(nodes);
    const [leader] = await waitForLeader(nodes);
    const follower = nodes.find((n) => n.id !== leader.id);
    const followerAddr = follower.addr;

    
    leader.network.blockPeer(followerAddr);
    follower.network.blockPeer(leader.addr);

    leader.handleSet('during', 'partition');
    await sleep(500);
    assert.notEqual(follower.stateMachine.get('during'), 'partition', 'partitioned follower should not see the write yet');

    
    leader.network.unblockPeer(followerAddr);
    follower.network.unblockPeer(leader.addr);

    await sleep(1000);
    assert.equal(follower.stateMachine.get('during'), 'partition', 'follower should catch up once healed');
  } finally {
    stopAll(nodes);
    fs.rmSync(dataDir, { recursive: true, force: true });
  }
});

test('persisted state survives a restart', async () => {
  const ports = nextPortSet();
  const dataDir = `.test-data-${ports[0]}`;
  const nodes = makeCluster(ports, dataDir);
  try {
    await startAll(nodes);
    const [leader] = await waitForLeader(nodes);
    leader.handleSet('durable', 'yes');
    await sleep(800);

    const termBefore = leader.currentTerm;
    const logLenBefore = leader.log.length;

    
    
    
    leader.stop();
    await sleep(100);

    const peerAddrs = { 1: nodes[0].addr, 2: nodes[1].addr, 3: nodes[2].addr };
    const restarted = new RaftNode(leader.id, leader.addr, peerAddrs, { dataDir });

    assert.equal(restarted.currentTerm, termBefore);
    assert.equal(restarted.log.length, logLenBefore);
    assert.equal(restarted.stateMachine.get('durable'), 'yes');
  } finally {
    stopAll(nodes);
    fs.rmSync(dataDir, { recursive: true, force: true });
  }
});
