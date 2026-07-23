import { randomTimeout } from './utils.js';




export class ElectionTimer {
  constructor(onTimeout, min = 800, max = 1500) {
    this.min = min;
    this.max = max;
    this.onTimeout = onTimeout;
    this.timer = null;
  }

  reset() {
    this.clear();
    const delay = randomTimeout(this.min, this.max);
    this.timer = setTimeout(() => {
      this.timer = null;
      this.onTimeout();
    }, delay);
  }

  clear() {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }
}


export class HeartbeatTicker {
  constructor(onTick, interval = 100) {
    this.interval = interval;
    this.onTick = onTick;
    this.timer = null;
    this.running = false;
  }

  start() {
    if (this.running) return;
    this.running = true;
    this.timer = setInterval(() => {
      if (this.running) this.onTick();
    }, this.interval);
  }

  stop() {
    this.running = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }
}
