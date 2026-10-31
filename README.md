# Cloudflare Workers + D1 Edge Distributed Systems Playground

A hybrid **Cloudflare Edge Control Plane (`src/worker.mjs` + Cloudflare D1 SQLite)** and **Multi-Process Mac Go Worker Cluster** that unifies all three distributed systems projects into an interactive web playground and CLI controller:

1. **Edge Saga Coordinator (D1 WAL + Tombstone Zombie Fencing + Circuit Breakers + DLQ Replay)**
2. **Edge Stream vs. Batch Reconciler (1 KB Base64-Serialized HyperLogLog Sketch in D1 vs. Exact SQL Batch)**
3. **Hybrid Go MapReduce Cluster (Edge Coordinator $\leftrightarrow$ Multi-Node Go HTTP Worker Processes on your Mac)**

---

## Architecture

```
+-----------------------------------------------------------------------------------+
|                   Cloudflare Worker Edge Isolate (src/worker.mjs)                 |
|  - Interactive Chaos Dashboard UI (GET /)                                         |
|  - Stateless Edge APIs (/api/saga/*, /api/reconciler/*, /api/mapreduce/*)         |
+--------------------+-----------------------------------------+--------------------+
                     |                                         |
                     | Cloudflare D1 Binding (env.DB)          | HTTP RPC (localhost or
                     v                                         | cloudflared Tunnel)
+-----------------------------------------+                    v
|      Cloudflare D1 Edge SQLite DB       |   +-------------------------------------+
|-----------------------------------------|   |  Mac Multi-Node Go HTTP Cluster     |
| - sagas / service_ledger                |   |  (cmd/cluster on :9090)             |
| - service_tombstones (Zombie Fencing)   |   |  +--> Worker 1 (127.0.0.1:9091)     |
| - dead_letter_queue / circuit_breakers  |   |  +--> Worker 2 (127.0.0.1:9092)     |
| - raw_events_batch (Batch Ground Truth) |   |  +--> Worker 3 (127.0.0.1:9093)     |
| - streaming_windows (1KB HLL Blobs)     |   |  +--> Worker 4 (127.0.0.1:9094)     |
+-----------------------------------------+   +-------------------------------------+
```

---

## Quick Start (3 Ways to Play from Your Mac)

### Mode 1: Instant Local Edge + Multi-Node Go Cluster (Zero Cloud Login Needed)

1. **Start the 4-Node Go HTTP MapReduce Cluster** (in Terminal 1):
   ```bash
   cd /Users/aditya.bilgundi/OSS/distributed-log-engine
   go run ./cmd/cluster
   ```
2. **Start the Cloudflare Worker + D1 Local Runtime** (in Terminal 2):
   ```bash
   cd /Users/aditya.bilgundi/OSS/cloudflare-distributed-lab
   node server.mjs
   ```
   Open **http://127.0.0.1:8787** in your browser, or run chaos scenarios from your Mac terminal:
   ```bash
   python3 mac_cli.py demo
   python3 mac_cli.py saga --count 50 --fail-rate 0.25 --zombie-rate 0.20 --force-dlq
   python3 mac_cli.py dlq-replay
   python3 mac_cli.py stream --events 1000 --drop-rate 0.06 --reconcile
   python3 mac_cli.py mapreduce --records 200000 --speculative --inject-crash --inject-straggler
   ```

---

### Mode 2: Expose Your Mac to the Public Internet via Free Cloudflare Tunnel (`cloudflared`)

Want a public `https://*.trycloudflare.com` URL routed through Cloudflare's global edge to your Mac in 5 seconds (no account or credit card required)?

```bash
brew install cloudflared
cloudflared tunnel --url http://127.0.0.1:8787
```

Cloudflare prints a live `https://<random-words>.trycloudflare.com` URL. You can open that URL on your phone/laptop or point `mac_cli.py` at it:
```bash
python3 mac_cli.py --url https://<your-tunnel>.trycloudflare.com demo
```

---

### Mode 3: Deploy 24/7 to Cloudflare Workers + D1 Free Tier (`wrangler`)

Because `src/worker.mjs` uses 100% native Cloudflare Worker ES Module syntax and `env.DB` D1 bindings:

```bash
cd /Users/aditya.bilgundi/OSS/cloudflare-distributed-lab

# 1. Authenticate with your free Cloudflare account
npx wrangler login

# 2. Create the free D1 SQLite database at the edge
npx wrangler d1 create distributed-lab-db
# -> Copy the returned database_id into wrangler.toml

# 3. Initialize the D1 schema remotely
npm run d1:init:remote

# 4. Deploy the Worker + Dashboard globally
npm run deploy
```

To bridge your deployed `*.workers.dev` edge worker to your Mac's Go MapReduce cluster when your Mac is awake:
1. Run `cloudflared tunnel --url http://127.0.0.1:9090` to expose the Go Coordinator.
2. Update `GO_CLUSTER_COORDINATOR_URL` in `wrangler.toml` to that tunnel URL and run `npm run deploy`.
