import NetworkLayer from './network.js';
import StateMachine from './stateMachine.js';
import Persistence from './persistence.js';
import { ElectionTimer, HeartbeatTicker } from './timers.js';

export default class RaftNode {
  constructor(id, addr, peerAddrs, opts = {}) {
    this.id = id;
    this.addr = addr;
    this.peerAddrs = peerAddrs;
    this.peerIds = Object.keys(peerAddrs).map(Number).filter((p) => p !== id);

    this.currentTerm = 0;
    this.votedFor = null;
    this.log = [];

    
    this.commitIndex = -1;
    this.nextIndex = {};
    this.matchIndex = {};
    this.state = 'follower';
    this.active = true;

    this.network = new NetworkLayer(addr);
    this.stateMachine = new StateMachine();
    this.persistence = new Persistence(id, opts.dataDir);

    const restored = this.persistence.load();
    if (restored) {
      this.currentTerm = restored.currentTerm;
      this.votedFor = restored.votedFor;
      this.log = restored.log;
      
      if (this.log.length > 0) {
        this.stateMachine.applyEntries(this.log, this.log.length - 1);
        this.commitIndex = this.log.length - 1;
      }
    }

    this.electionTimer = new ElectionTimer(() => this._startElection());
    this.heartbeatTicker = new HeartbeatTicker(() => this._sendHeartbeat());

    this.network.on('rpc', (msg, socket) => this._handleRPC(msg, socket));
  }

 
  _persist() {
    if (!this.active) return; 
    this.persistence.save({
      currentTerm: this.currentTerm,
      votedFor: this.votedFor,
      log: this.log,
    });
  }

  start() {
    return this.network.start().then(() => {
      this.electionTimer.reset();
      console.log(`[Node ${this.id}] ready at ${this.addr}, term=${this.currentTerm}, logLen=${this.log.length}`);
    });
  }

  stop() {
    this.active = false;
    this.network.stop();
    this.electionTimer.clear();
    this.heartbeatTicker.stop();
  }

 
  crash() {
    if (!this.active) return;
    this.active = false;
    this.state = 'follower';
    this.electionTimer.clear();
    this.heartbeatTicker.stop();
    console.log(`[Node ${this.id}] crashed`);
  }

  recover() {
    if (this.active) return;
    this.active = true;
    this.state = 'follower';
    this.electionTimer.reset();
    console.log(`[Node ${this.id}] recovered`);
  }

  handleSet(key, value) {
    if (this.state !== 'leader') {
      console.log(`[Node ${this.id}] not leader, cannot accept write`);
      return false;
    }
    this.log.push({ term: this.currentTerm, command: `set ${key} ${value}` });
    this._persist();
    this._sendHeartbeat();
    console.log(`[Node ${this.id}] accepted write ${key}=${value}`);
    return true;
  }

  handleGet(key) {
    const val = this.stateMachine.get(key);
    console.log(`[Node ${this.id}] get ${key} = ${val !== undefined ? val : '(not found)'}`);
    return val;
  }

  _handleRPC(msg, socket) {
    if (!this.active) return; 
    const { type, args } = msg;
    let reply;
    if (type === 'RequestVote') {
      reply = this._handleRequestVote(args);
    } else if (type === 'AppendEntries') {
      reply = this._handleAppendEntries(args);
    } else {
      console.warn(`[Node ${this.id}] unknown RPC type ${type}`);
      return;
    }
    socket.write(JSON.stringify(reply) + '\n');
  }

  _handleRequestVote(args) {
    const { term, candidateId, lastLogIndex, lastLogTerm } = args;
    const reply = { term: this.currentTerm, voteGranted: false };

    if (term < this.currentTerm) return reply;

    if (term > this.currentTerm) {
      this._stepDown(term);
    }

    
    if (this.votedFor !== null && this.votedFor !== candidateId) {
      reply.term = this.currentTerm;
      return reply;
    }

    
    
    const lastIdx = this.log.length - 1;
    const lastTerm = lastIdx >= 0 ? this.log[lastIdx].term : 0;
    const upToDate =
      lastLogTerm > lastTerm || (lastLogTerm === lastTerm && lastLogIndex >= lastIdx);
    if (!upToDate) {
      reply.term = this.currentTerm;
      return reply;
    }

    this.votedFor = candidateId;
    this._persist();
    this.electionTimer.reset();
    reply.term = this.currentTerm;
    reply.voteGranted = true;
    return reply;
  }

  _handleAppendEntries(args) {
    const { term, prevLogIndex, prevLogTerm, entries, leaderCommit, leaderId } = args;
    const reply = { term: this.currentTerm, success: false, conflictIndex: 0, conflictTerm: 0 };

    if (term < this.currentTerm) return reply;

    if (term > this.currentTerm) {
      this._stepDown(term);
    } else if (this.state !== 'follower') {
      
      
      this.state = 'follower';
      this.heartbeatTicker.stop();
    }

    this.electionTimer.reset();

    if (entries.length > 0) {
      console.log(`[Node ${this.id}] got ${entries.length} entries from leader ${leaderId}`);
    }

    
    if (prevLogIndex > this.log.length - 1) {
      reply.conflictIndex = this.log.length;
      reply.conflictTerm = -1;
      reply.term = this.currentTerm;
      return reply;
    }
    if (prevLogIndex >= 0 && this.log[prevLogIndex].term !== prevLogTerm) {
      const conflictTerm = this.log[prevLogIndex].term;
      let idx = prevLogIndex;
      while (idx > 0 && this.log[idx - 1].term === conflictTerm) idx--;
      reply.conflictIndex = idx;
      reply.conflictTerm = conflictTerm;
      reply.term = this.currentTerm;
      return reply;
    }

    
    let changed = false;
    for (let i = 0; i < entries.length; i++) {
      const idx = prevLogIndex + 1 + i;
      if (idx < this.log.length) {
        if (this.log[idx].term !== entries[i].term) {
          this.log = this.log.slice(0, idx);
          this.log.push({ term: entries[i].term, command: entries[i].command });
          changed = true;
        }
        
      } else {
        this.log.push({ term: entries[i].term, command: entries[i].command });
        changed = true;
      }
    }
    if (changed) this._persist();

    if (leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(leaderCommit, this.log.length - 1);
      this.stateMachine.applyEntries(this.log, this.commitIndex);
    }

    reply.success = true;
    reply.term = this.currentTerm;
    return reply;
  }

  _stepDown(newTerm) {
    this.currentTerm = newTerm;
    this.state = 'follower';
    this.votedFor = null;
    this._persist();
    this.heartbeatTicker.stop();
  }

  _startElection() {
    if (!this.active || this.state === 'leader') return;

    this.currentTerm++;
    this.state = 'candidate';
    this.votedFor = this.id;
    this._persist();
    this.electionTimer.reset();

    console.log(`[Node ${this.id}] starting election for term ${this.currentTerm}`);

    let votes = 1; 
    const electionTerm = this.currentTerm;

    const requests = this.peerIds.map((peerId) => {
      const args = {
        term: this.currentTerm,
        candidateId: this.id,
        lastLogIndex: this.log.length - 1,
        lastLogTerm: this.log.length > 0 ? this.log[this.log.length - 1].term : 0,
      };
      return this.network
        .sendRPC(this.peerAddrs[peerId], 'RequestVote', args)
        .then((reply) => {
          if (reply.term > this.currentTerm) {
            this._stepDown(reply.term);
            return;
          }
          if (reply.voteGranted && this.currentTerm === electionTerm) votes++;
        })
        .catch(() => {}); 
    });

    Promise.all(requests).then(() => {
      
      if (this.state !== 'candidate' || this.currentTerm !== electionTerm) return;

      const clusterSize = this.peerIds.length + 1;
      const majority = Math.floor(clusterSize / 2) + 1;

      if (votes >= majority) {
        console.log(`[Node ${this.id}] won election for term ${this.currentTerm} with ${votes} votes`);
        this.electionTimer.clear();
        this.state = 'leader';
        for (const p of this.peerIds) {
          this.nextIndex[p] = this.log.length;
          this.matchIndex[p] = 0;
        }
        this.heartbeatTicker.start();
        this._sendHeartbeat();
      }
      
      
    });
  }

  _sendHeartbeat() {
    if (!this.active || this.state !== 'leader') return;
    for (const peerId of this.peerIds) {
      const args = this._buildAppendEntriesArgs(peerId);
      this.network
        .sendRPC(this.peerAddrs[peerId], 'AppendEntries', args)
        .then((reply) => this._handleAppendEntriesReply(peerId, args, reply))
        .catch(() => {}); 
    }
  }

  _buildAppendEntriesArgs(peerId) {
    const nextIdx = this.nextIndex[peerId] ?? this.log.length;
    const prevLogIndex = nextIdx - 1;
    const prevLogTerm = prevLogIndex >= 0 ? this.log[prevLogIndex].term : 0;
    const entries = nextIdx < this.log.length
      ? this.log.slice(nextIdx).map((e) => ({ term: e.term, command: e.command }))
      : [];
    return {
      term: this.currentTerm,
      leaderId: this.id,
      prevLogIndex,
      prevLogTerm,
      entries,
      leaderCommit: this.commitIndex,
    };
  }

  _handleAppendEntriesReply(peerId, args, reply) {
    if (this.state !== 'leader') return;

    if (reply.term > this.currentTerm) {
      this._stepDown(reply.term);
      return;
    }

    if (reply.success) {
      this.matchIndex[peerId] = args.prevLogIndex + args.entries.length;
      this.nextIndex[peerId] = this.matchIndex[peerId] + 1;

      
      
      
      for (let idx = this.commitIndex + 1; idx < this.log.length; idx++) {
        if (this.log[idx].term !== this.currentTerm) continue;
        let count = 1;
        for (const p of this.peerIds) {
          if ((this.matchIndex[p] ?? -1) >= idx) count++;
        }
        if (count > (this.peerIds.length + 1) / 2) {
          this.commitIndex = idx;
        }
      }
      this.stateMachine.applyEntries(this.log, this.commitIndex);
    } else {
      if (reply.conflictIndex >= 0) {
        this.nextIndex[peerId] = reply.conflictIndex;
      } else {
        this.nextIndex[peerId] = Math.max(0, (this.nextIndex[peerId] ?? 1) - 1);
      }
    }
  }
}
