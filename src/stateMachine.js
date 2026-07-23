



export default class StateMachine {
  constructor() {
    this.kvStore = new Map();
    this.lastApplied = -1;
  }

  applyEntries(log, commitIndex) {
    while (this.lastApplied < commitIndex) {
      this.lastApplied++;
      const entry = log[this.lastApplied];
      if (!entry) break; 
      this._apply(entry.command);
    }
  }

  _apply(command) {
    const parts = command.split(' ');
    if (parts[0] === 'set' && parts.length === 3) {
      const [, key, value] = parts;
      this.kvStore.set(key, value);
    } else if (parts[0] === 'delete' && parts.length === 2) {
      this.kvStore.delete(parts[1]);
    } else {
      console.warn(`[state] unrecognized command: ${command}`);
    }
  }

  get(key) {
    return this.kvStore.get(key);
  }

  dump() {
    return Object.fromEntries(this.kvStore);
  }
}
