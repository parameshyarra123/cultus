import readline from 'readline';
import RaftNode from './src/node.js';

const args = process.argv.slice(2);
if (args.length < 2) {
  console.log('Usage: node index.js <node-id> <addr:port> [peerId=addr:port ...]');
  console.log('Example: node index.js 1 localhost:8001 2=localhost:8002 3=localhost:8003');
  process.exit(1);
}

const id = parseInt(args[0], 10);
const addr = args[1];
const peerAddrs = {};
for (let i = 2; i < args.length; i++) {
  const [peerId, peerAddr] = args[i].split('=');
  peerAddrs[parseInt(peerId, 10)] = peerAddr;
}

const node = new RaftNode(id, addr, peerAddrs);

node
  .start()
  .then(() => {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout, terminal: true });

    console.log('Commands: set <key> <value> | get <key> | dump | crash | recover | partition <peerId> | heal <peerId> | latency <ms> | loss <rate> | exit');

    rl.on('line', (line) => {
      const parts = line.trim().split(' ');
      const cmd = parts[0];

      if (cmd === 'set' && parts.length === 3) {
        node.handleSet(parts[1], parts[2]);
      } else if (cmd === 'get' && parts.length === 2) {
        node.handleGet(parts[1]);
      } else if (cmd === 'dump') {
        console.log(JSON.stringify(node.stateMachine.dump()));
      } else if (cmd === 'crash') {
        node.crash();
      } else if (cmd === 'recover') {
        node.recover();
      } else if (cmd === 'partition' && parts.length === 2) {
        const peerAddr = peerAddrs[parseInt(parts[1], 10)];
        if (peerAddr) node.network.blockPeer(peerAddr);
      } else if (cmd === 'heal' && parts.length === 2) {
        const peerAddr = peerAddrs[parseInt(parts[1], 10)];
        if (peerAddr) node.network.unblockPeer(peerAddr);
      } else if (cmd === 'latency' && parts.length === 2) {
        node.network.setLatency(parseInt(parts[1], 10));
      } else if (cmd === 'loss' && parts.length === 2) {
        node.network.setPacketLoss(parseFloat(parts[1]));
      } else if (cmd === 'exit' || cmd === 'quit') {
        rl.close();
      } else {
        console.log(`[Node ${id}] unknown command`);
      }
    });

    rl.on('close', () => {
      node.stop();
      process.exit(0);
    });
  })
  .catch((err) => {
    console.error('Failed to start node:', err);
    process.exit(1);
  });
