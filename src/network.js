import net from 'net';
import { EventEmitter } from 'events';




export default class NetworkLayer extends EventEmitter {
  constructor(addr) {
    super();
    this.addr = addr;
    this.server = null;
    this.latencyMs = 20;
    this.packetLossRate = 0;
    this.blockedPeers = new Set();
  }

  start() {
    return new Promise((resolve, reject) => {
      this.server = net.createServer((socket) => this._handleSocket(socket));
      const [host, port] = this.addr.split(':');
      this.server.listen(parseInt(port, 10), host, () => resolve());
      this.server.on('error', reject);
    });
  }

  stop() {
    if (this.server) {
      this.server.close();
      this.server = null;
    }
  }

  sendRPC(peerAddr, type, args, timeoutMs = 1000) {
    return new Promise((resolve, reject) => {
      if (this.blockedPeers.has(peerAddr)) {
        reject(new Error('network partition'));
        return;
      }
      if (Math.random() < this.packetLossRate) {
        reject(new Error('packet dropped'));
        return;
      }

      const [host, port] = peerAddr.split(':');
      const socket = net.createConnection(parseInt(port, 10), host);

      const timer = setTimeout(() => {
        socket.destroy();
        reject(new Error('RPC timeout'));
      }, timeoutMs);

      let buffer = '';
      socket.on('data', (chunk) => {
        buffer += chunk.toString();
        const idx = buffer.indexOf('\n');
        if (idx !== -1) {
          const line = buffer.slice(0, idx);
          clearTimeout(timer);
          socket.destroy();
          try {
            resolve(JSON.parse(line));
          } catch (e) {
            reject(e);
          }
        }
      });

      socket.on('error', (err) => {
        clearTimeout(timer);
        socket.destroy();
        reject(err);
      });

      const msg = JSON.stringify({ type, args });
      setTimeout(() => {
        if (!socket.destroyed) socket.write(msg + '\n');
      }, this.latencyMs);
    });
  }

  _handleSocket(socket) {
    let buffer = '';
    socket.on('data', (chunk) => {
      buffer += chunk.toString();
      let boundary = buffer.indexOf('\n');
      while (boundary !== -1) {
        const msgStr = buffer.slice(0, boundary);
        buffer = buffer.slice(boundary + 1);
        try {
          const msg = JSON.parse(msgStr);
          this.emit('rpc', msg, socket);
        } catch (e) {
          console.error('[network] bad JSON on wire:', msgStr);
        }
        boundary = buffer.indexOf('\n');
      }
    });
    socket.on('error', () => {}); 
  }

  setLatency(ms) {
    this.latencyMs = ms;
  }

  setPacketLoss(rate) {
    this.packetLossRate = rate;
  }

  blockPeer(addr) {
    this.blockedPeers.add(addr);
  }

  unblockPeer(addr) {
    this.blockedPeers.delete(addr);
  }

  isBlocked(addr) {
    return this.blockedPeers.has(addr);
  }
}
