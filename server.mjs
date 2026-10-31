import http from "node:http";
import { spawn } from "node:child_process";
import readline from "node:readline";
import path from "node:path";
import { fileURLToPath } from "node:url";
import worker from "./src/worker.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT || 8787);
const GO_CLUSTER_URL =
  process.env.GO_CLUSTER_COORDINATOR_URL || "http://127.0.0.1:9090";

class LocalD1Database {
  constructor() {
    this.nextId = 1;
    this.pending = new Map();
    this.proc = spawn(
      "python3",
      [path.join(__dirname, "scripts", "d1_sqlite_bridge.py")],
      { stdio: ["pipe", "pipe", "inherit"] }
    );
    const rl = readline.createInterface({ input: this.proc.stdout });
    rl.on("line", (line) => {
      if (!line.trim()) return;
      const msg = JSON.parse(line);
      const entry = this.pending.get(msg.id);
      if (entry) {
        this.pending.delete(msg.id);
        if (msg.ok) entry.resolve(msg.result);
        else entry.reject(new Error(msg.error));
      }
    });
  }

  _exec(op, sql, params) {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.proc.stdin.write(JSON.stringify({ id, op, sql, params }) + "\n");
    });
  }

  prepare(sql) {
    const db = this;
    class Statement {
      constructor(query, bound = []) {
        this.query = query;
        this.bound = bound;
      }
      bind(...params) {
        return new Statement(this.query, params);
      }
      all() {
        return db._exec("all", this.query, this.bound);
      }
      first() {
        return db._exec("first", this.query, this.bound);
      }
      run() {
        return db._exec("run", this.query, this.bound);
      }
    }
    return new Statement(sql);
  }
}

const d1 = new LocalD1Database();
const env = {
  DB: d1,
  GO_CLUSTER_COORDINATOR_URL: GO_CLUSTER_URL,
};

const server = http.createServer(async (req, res) => {
  try {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const bodyBuf = Buffer.concat(chunks);

    const fullUrl = `http://${req.headers.host || `127.0.0.1:${PORT}`}${req.url}`;
    const init = {
      method: req.method,
      headers: req.headers,
    };
    if (req.method !== "GET" && req.method !== "HEAD" && bodyBuf.length > 0) {
      init.body = bodyBuf;
    }

    const webReq = new Request(fullUrl, init);
    const webResp = await worker.fetch(webReq, env, { waitUntil: () => {} });

    res.statusCode = webResp.status;
    webResp.headers.forEach((val, key) => res.setHeader(key, val));
    const respBuf = Buffer.from(await webResp.arrayBuffer());
    res.end(respBuf);
  } catch (err) {
    res.statusCode = 500;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ error: String(err) }));
  }
});

server.listen(PORT, "127.0.0.1", () => {
  console.log(
    `[Cloudflare Worker + D1 Local Runtime] Listening on http://127.0.0.1:${PORT}`
  );
});
