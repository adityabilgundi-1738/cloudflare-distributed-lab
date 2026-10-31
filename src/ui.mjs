export function renderDashboardHTML() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Cloudflare Edge + D1 Distributed Systems Chaos Lab</title>
  <style>
    :root {
      --bg: #0b0f19;
      --panel: #131b2e;
      --border: #23304f;
      --text: #e2e8f0;
      --muted: #94a3b8;
      --accent: #f97316;
      --blue: #38bdf8;
      --green: #22c55e;
      --red: #ef4444;
      --purple: #a855f7;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace;
      background: var(--bg);
      color: var(--text);
      padding: 20px;
    }
    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      border-bottom: 1px solid var(--border);
      padding-bottom: 14px;
      margin-bottom: 20px;
      flex-wrap: wrap;
      gap: 12px;
    }
    .title-group h1 {
      margin: 0;
      font-size: 20px;
      color: #fff;
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .badge {
      background: rgba(249, 115, 22, 0.15);
      color: var(--accent);
      border: 1px solid var(--accent);
      font-size: 11px;
      padding: 3px 8px;
      border-radius: 999px;
      font-weight: 600;
    }
    .badge.ok {
      background: rgba(34, 197, 94, 0.15);
      color: var(--green);
      border-color: var(--green);
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(410px, 1fr));
      gap: 18px;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 16px;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    .card h2 {
      margin: 0;
      font-size: 15px;
      color: var(--blue);
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .controls {
      display: grid;
      grid-template-columns: repeat(2, 1fr);
      gap: 8px;
      background: rgba(0, 0, 0, 0.25);
      padding: 10px;
      border-radius: 8px;
    }
    .control-item {
      display: flex;
      flex-direction: column;
      gap: 4px;
      font-size: 11px;
      color: var(--muted);
    }
    input[type="number"], select {
      background: #0b0f19;
      border: 1px solid var(--border);
      color: #fff;
      padding: 6px 8px;
      border-radius: 5px;
      font-family: inherit;
      font-size: 12px;
    }
    .btn-row {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    button {
      background: var(--accent);
      color: #fff;
      border: none;
      padding: 8px 12px;
      border-radius: 6px;
      cursor: pointer;
      font-weight: 600;
      font-size: 12px;
      font-family: inherit;
    }
    button.secondary {
      background: #1e293b;
      border: 1px solid var(--border);
      color: var(--text);
    }
    button:hover { opacity: 0.92; }
    .metrics-strip {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 8px;
    }
    .stat {
      background: rgba(255, 255, 255, 0.03);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 8px;
    }
    .stat-label { font-size: 10px; color: var(--muted); }
    .stat-val { font-size: 15px; font-weight: 700; margin-top: 3px; color: #fff; }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 11px;
    }
    th, td {
      text-align: left;
      padding: 6px;
      border-bottom: 1px solid rgba(255,255,255,0.06);
    }
    th { color: var(--muted); font-weight: 600; }
    .cli-box {
      background: #060911;
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 12px;
      font-size: 11px;
      color: #a5f3fc;
      overflow-x: auto;
      white-space: pre;
      margin-top: 16px;
    }
  </style>
</head>
<body>
  <header>
    <div class="title-group">
      <h1>
        Cloudflare Worker + D1 Distributed Systems Lab
        <span class="badge">Edge D1 SQLite Active</span>
        <span id="consistency-badge" class="badge ok">Inconsistent State: 0.0000%</span>
      </h1>
      <div style="font-size: 12px; color: var(--muted); margin-top: 4px;">
        Stateful Edge Coordination (Cloudflare Workers + D1) &harr; Multi-Process Mac Go Worker Cluster
      </div>
    </div>
    <div class="btn-row">
      <button class="secondary" onclick="refreshState()">Refresh D1 Telemetry</button>
      <button class="secondary" onclick="resetAll()">Reset D1 State</button>
    </div>
  </header>

  <div class="grid">
    <!-- 1. SAGA COORDINATOR -->
    <div class="card">
      <h2>
        <span>1. Edge Saga Coordinator (D1 WAL + Zombie Fencing)</span>
        <span class="badge" id="cb-badge">CB: CLOSED</span>
      </h2>
      <div class="controls">
        <div class="control-item">
          <label>Batch Sagas to Execute</label>
          <input type="number" id="saga-count" value="25" min="1" max="200" />
        </div>
        <div class="control-item">
          <label>Step Failure Rate (0-1)</label>
          <input type="number" id="saga-fail" value="0.20" step="0.05" min="0" max="1" />
        </div>
        <div class="control-item">
          <label>Zombie RPC Race Rate (0-1)</label>
          <input type="number" id="saga-zombie" value="0.15" step="0.05" min="0" max="1" />
        </div>
        <div class="control-item">
          <label>Simulate Compensation Outage (DLQ)</label>
          <select id="saga-dlq">
            <option value="false">Normal Compensation</option>
            <option value="true">Force Compensation Outage -> DLQ</option>
          </select>
        </div>
      </div>
      <div class="btn-row">
        <button onclick="runSagas()">Execute Sagas on Edge D1</button>
        <button class="secondary" onclick="replayDLQ()">Replay D1 Dead-Letter Queue (<span id="dlq-count">0</span>)</button>
      </div>
      <div class="metrics-strip">
        <div class="stat"><div class="stat-label">Total Sagas</div><div class="stat-val" id="s-total">0</div></div>
        <div class="stat"><div class="stat-label">Success Rate</div><div class="stat-val" id="s-rate">0%</div></div>
        <div class="stat"><div class="stat-label">Zombies Fenced</div><div class="stat-val" id="s-zombies">0</div></div>
        <div class="stat"><div class="stat-label">Avg Rollback</div><div class="stat-val" id="s-rollback">0 ms</div></div>
      </div>
      <div style="max-height: 190px; overflow-y: auto;">
        <table>
          <thead><tr><th>Saga ID</th><th>Status</th><th>Failed Step</th><th>Zombie Fence</th><th>Rollback (µs)</th></tr></thead>
          <tbody id="saga-table"></tbody>
        </table>
      </div>
    </div>

    <!-- 2. STREAM VS BATCH RECONCILER -->
    <div class="card">
      <h2>
        <span>2. Stream vs. Batch Reconciler (1KB D1 HLL Sketch)</span>
        <span class="badge ok">1024-Reg HLL (1 KB/win)</span>
      </h2>
      <div class="controls">
        <div class="control-item">
          <label>Events to Stream</label>
          <input type="number" id="rec-events" value="600" min="50" max="5000" />
        </div>
        <div class="control-item">
          <label>Window Fault Mode</label>
          <select id="rec-mode">
            <option value="ALTERNATING">Alternating (Odd Windows Faulty)</option>
            <option value="DUP_ONLY">Duplicate Delivery Burst</option>
            <option value="DROP_ONLY">Packet Drop / Late Burst</option>
          </select>
        </div>
        <div class="control-item">
          <label>Drop / Late Rate (0-1)</label>
          <input type="number" id="rec-drop" value="0.06" step="0.01" />
        </div>
        <div class="control-item">
          <label>Duplicate Rate (0-1)</label>
          <input type="number" id="rec-dup" value="0.06" step="0.01" />
        </div>
      </div>
      <div class="btn-row">
        <button onclick="ingestStream()">1. Stream Events to Edge</button>
        <button class="secondary" onclick="runReconcile(true)">2. Reconcile & Auto-Correct from Batch</button>
      </div>
      <div class="metrics-strip">
        <div class="stat"><div class="stat-label">Raw Batch Events</div><div class="stat-val" id="r-events">0</div></div>
        <div class="stat"><div class="stat-label">Flagged Windows</div><div class="stat-val" id="r-flagged">0</div></div>
        <div class="stat"><div class="stat-label">Root-Cause Acc</div><div class="stat-val" id="r-rc">100%</div></div>
        <div class="stat"><div class="stat-label">Post-Fix Drift</div><div class="stat-val" id="r-post">0.00%</div></div>
      </div>
      <div style="max-height: 190px; overflow-y: auto;">
        <table>
          <thead><tr><th>Win</th><th>Stream Cnt</th><th>Batch Cnt</th><th>HLL Users</th><th>Diagnosed Cause</th><th>Fixed</th></tr></thead>
          <tbody id="rec-table"></tbody>
        </table>
      </div>
    </div>

    <!-- 3. HYBRID GO MAPREDUCE CLUSTER -->
    <div class="card">
      <h2>
        <span>3. Hybrid Go MapReduce Cluster (Real Mac OS Workers)</span>
        <span class="badge" id="mr-cluster-badge">Cluster Ready</span>
      </h2>
      <div class="controls">
        <div class="control-item">
          <label>Log Records</label>
          <input type="number" id="mr-records" value="200000" step="50000" />
        </div>
        <div class="control-item">
          <label>Speculative Backup Tasks</label>
          <select id="mr-spec">
            <option value="true">ENABLED (Hedged Backup Tasks)</option>
            <option value="false">DISABLED (Wait on Stragglers)</option>
          </select>
        </div>
        <div class="control-item">
          <label>Inject Worker Crash</label>
          <select id="mr-crash">
            <option value="true">Crash Worker on Chunk #2 & #5</option>
            <option value="false">No Worker Crashes</option>
          </select>
        </div>
        <div class="control-item">
          <label>Inject 75ms Straggler Stall</label>
          <select id="mr-straggler">
            <option value="true">Stall Chunk #1 & #4 (75ms)</option>
            <option value="false">No Stragglers</option>
          </select>
        </div>
      </div>
      <div class="btn-row">
        <button onclick="runMapReduce()">Dispatch MapReduce Job to Mac Worker Cluster</button>
      </div>
      <div class="metrics-strip">
        <div class="stat"><div class="stat-label">Throughput</div><div class="stat-val" id="m-tput">0 rec/s</div></div>
        <div class="stat"><div class="stat-label">Wall Time</div><div class="stat-val" id="m-wall">0 ms</div></div>
        <div class="stat"><div class="stat-label">Backup Wins</div><div class="stat-val" id="m-spec">0</div></div>
        <div class="stat"><div class="stat-label">Data Loss</div><div class="stat-val" id="m-loss">0.0000%</div></div>
      </div>
      <div style="max-height: 190px; overflow-y: auto;">
        <table>
          <thead><tr><th>Job ID</th><th>Mode</th><th>Wall (ms)</th><th>Speedup</th><th>Crashes Fixed</th><th>Spec Wins</th><th>Loss %</th></tr></thead>
          <tbody id="mr-table"></tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="cli-box" id="cli-helper">
# Play from your Mac Terminal (works against localhost:8787 OR your *.workers.dev / *.trycloudflare.com URL):
python3 mac_cli.py status
python3 mac_cli.py saga --count 50 --fail-rate 0.25 --zombie-rate 0.20
python3 mac_cli.py stream --events 1000 --drop-rate 0.05 --reconcile
python3 mac_cli.py mapreduce --records 300000 --speculative
  </div>

  <script>
    async function api(path, method = "GET", body = null) {
      const opts = { method, headers: { "Content-Type": "application/json" } };
      if (body) opts.body = JSON.stringify(body);
      const res = await fetch(path, opts);
      return res.json();
    }

    async function refreshState() {
      const data = await api("/api/state");
      // Update Saga UI
      document.getElementById("s-total").textContent = data.saga.total;
      document.getElementById("s-rate").textContent = data.saga.success_rate_pct + "%";
      document.getElementById("s-zombies").textContent = data.saga.zombies_fenced;
      document.getElementById("s-rollback").textContent = data.saga.avg_rollback_ms + " ms";
      document.getElementById("dlq-count").textContent = data.saga.dlq_pending;
      document.getElementById("consistency-badge").textContent =
        "Inconsistent State: " + data.saga.inconsistent_pct + "% | Stock Valid: " + data.saga.stock_valid;

      const openCbs = data.saga.circuit_breakers.filter(c => c.state !== "CLOSED");
      const cbBadge = document.getElementById("cb-badge");
      cbBadge.textContent = openCbs.length ? ("CB OPEN (" + openCbs[0].service_name + ")") : "CB: ALL CLOSED";

      const sBody = document.getElementById("saga-table");
      sBody.innerHTML = data.saga.recent.map(r => \`
        <tr>
          <td>\${r.saga_id}</td>
          <td style="color:\${r.status === 'COMPLETED' ? '#22c55e' : (r.status === 'COMPENSATED' ? '#38bdf8' : '#ef4444')}">\${r.status}</td>
          <td>\${r.failed_step || '-'}</td>
          <td>\${r.zombie_fenced ? 'FENCED' : '-'}</td>
          <td>\${r.rollback_latency_us ? r.rollback_latency_us.toFixed(1) : '-'}</td>
        </tr>
      \`).join("");

      // Update Reconciler UI
      document.getElementById("r-events").textContent = data.reconciler.total_batch_events;
      document.getElementById("r-flagged").textContent = data.reconciler.last_run ? data.reconciler.last_run.flagged_windows : 0;
      document.getElementById("r-rc").textContent = (data.reconciler.last_run ? data.reconciler.last_run.root_cause_accuracy_pct : 100) + "%";
      document.getElementById("r-post").textContent = (data.reconciler.last_run ? data.reconciler.last_run.mean_drift_after_pct : 0) + "%";

      const rBody = document.getElementById("rec-table");
      rBody.innerHTML = data.reconciler.windows.map(w => \`
        <tr>
          <td>W#\${w.window_id}</td>
          <td>\${w.stream_count}</td>
          <td>\${w.batch_count}</td>
          <td>\${w.hll_users} (exact \${w.batch_users})</td>
          <td style="color:\${w.diagnosed_anomaly === 'NONE' ? '#94a3b8' : '#f97316'}">\${w.diagnosed_anomaly}</td>
          <td>\${w.auto_corrected ? 'YES' : '-'}</td>
        </tr>
      \`).join("");

      // Update MapReduce UI
      if (data.mapreduce.jobs.length > 0) {
        const latest = data.mapreduce.jobs[0];
        document.getElementById("m-tput").textContent = (latest.throughput_rec_sec / 1e6).toFixed(2) + "M/s";
        document.getElementById("m-wall").textContent = latest.wall_time_ms.toFixed(1) + " ms";
        document.getElementById("m-spec").textContent = latest.speculative_wins;
        document.getElementById("m-loss").textContent = latest.data_loss_pct.toFixed(4) + "%";
      }
      document.getElementById("mr-cluster-badge").textContent =
        data.mapreduce.cluster_online ? ("Mac Cluster Online (" + data.mapreduce.worker_nodes.length + " nodes)") : "In-Edge Fallback";

      const mBody = document.getElementById("mr-table");
      mBody.innerHTML = data.mapreduce.jobs.map(j => \`
        <tr>
          <td>\${j.job_id}</td>
          <td>\${j.mode}</td>
          <td>\${j.wall_time_ms.toFixed(2)}</td>
          <td>\${j.speedup.toFixed(2)}x</td>
          <td>\${j.failures_recovered}</td>
          <td>\${j.speculative_wins}</td>
          <td style="color:#22c55e">\${j.data_loss_pct.toFixed(4)}%</td>
        </tr>
      \`).join("");
    }

    async function runSagas() {
      await api("/api/saga/execute", "POST", {
        count: Number(document.getElementById("saga-count").value),
        fail_rate: Number(document.getElementById("saga-fail").value),
        zombie_rate: Number(document.getElementById("saga-zombie").value),
        force_dlq: document.getElementById("saga-dlq").value === "true"
      });
      await refreshState();
    }

    async function replayDLQ() {
      await api("/api/saga/dlq/replay", "POST", {});
      await refreshState();
    }

    async function ingestStream() {
      await api("/api/reconciler/ingest", "POST", {
        events: Number(document.getElementById("rec-events").value),
        mode: document.getElementById("rec-mode").value,
        drop_rate: Number(document.getElementById("rec-drop").value),
        duplicate_rate: Number(document.getElementById("rec-dup").value)
      });
      await refreshState();
    }

    async function runReconcile(autoCorrect) {
      await api("/api/reconciler/reconcile", "POST", { auto_correct: autoCorrect, threshold_pct: 1.0 });
      await refreshState();
    }

    async function runMapReduce() {
      await api("/api/mapreduce/run", "POST", {
        records: Number(document.getElementById("mr-records").value),
        speculative: document.getElementById("mr-spec").value === "true",
        inject_crash: document.getElementById("mr-crash").value === "true",
        inject_straggler: document.getElementById("mr-straggler").value === "true"
      });
      await refreshState();
    }

    async function resetAll() {
      await api("/api/reset", "POST", {});
      await refreshState();
    }

    refreshState();
  </script>
</body>
</html>`;
}
