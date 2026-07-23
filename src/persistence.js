import fs from 'fs';
import path from 'path';








export default class Persistence {
  constructor(nodeId, dataDir = '.raft-data') {
    this.dataDir = dataDir;
    this.file = path.join(dataDir, `node-${nodeId}.json`);
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true });
    }
  }

  load() {
    if (!fs.existsSync(this.file)) return null;
    try {
      const raw = fs.readFileSync(this.file, 'utf8');
      const parsed = JSON.parse(raw);
      if (typeof parsed.currentTerm !== 'number' || !Array.isArray(parsed.log)) {
        return null;
      }
      return parsed;
    } catch (err) {
      
      
      console.warn(`[persistence] could not read ${this.file}, starting fresh:`, err.message);
      return null;
    }
  }

  save(state) {
    const tmpFile = `${this.file}.tmp`;
    
    
    fs.writeFileSync(tmpFile, JSON.stringify(state));
    fs.renameSync(tmpFile, this.file);
  }

  wipe() {
    if (fs.existsSync(this.file)) fs.unlinkSync(this.file);
  }
}
