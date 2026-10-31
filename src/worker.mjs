import {
  createEmptyHLL,
  deserializeHLL,
  hllAdd,
  hllCount,
  serializeHLL,
} from "./hll.mjs";
import { renderDashboardHTML } from "./ui.mjs";

const SKUS = ["SKU-LAPTOP", "SKU-PHONE", "SKU-MONITOR"];

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    },
  });
}

async function getSystemState(env, goCoordinatorUrl) {
  const sagaRows = (
    await env.DB.prepare(
      "SELECT * FROM sagas ORDER BY created_at_ms DESC LIMIT 25"
    ).all()
  ).results;

  const sagaCounts = await env.DB.prepare(
    `SELECT
       COUNT(*) AS total,
       SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed,
       SUM(CASE WHEN status = 'COMPENSATED' THEN 1 ELSE 0 END) AS compensated,
       SUM(CASE WHEN status = 'FAILED_DLQ' THEN 1 ELSE 0 END) AS failed_dlq,
       SUM(zombie_fenced) AS zombies_fenced,
       AVG(CASE WHEN rollback_latency_us > 0 THEN rollback_latency_us ELSE NULL END) AS avg_rollback_us
     FROM sagas`
  ).first();

  const dlqPending = await env.DB.prepare(
    "SELECT COUNT(*) AS cnt FROM dead_letter_queue WHERE resolved = 0"
  ).first();

  const stockRows = (
    await env.DB.prepare("SELECT * FROM inventory_stock").all()
  ).results;

  const ledgerRows = (
    await env.DB.prepare(
      `SELECT l.*, s.status AS saga_status
       FROM service_ledger l
       JOIN sagas s ON s.saga_id = l.saga_id`
    ).all()
  ).results;

  let inconsistentCount = 0;
  for (const row of ledgerRows) {
    if (row.saga_status === "COMPLETED") {
      if (
        row.inv_status !== "RESERVED" ||
        row.pay_status !== "CHARGED" ||
        row.ship_status !== "SCHEDULED"
      ) {
        inconsistentCount++;
      }
    } else if (row.saga_status === "COMPENSATED") {
      const invOk = row.inv_status === "NONE" || row.inv_status === "RELEASED";
      const payOk = row.pay_status === "NONE" || row.pay_status === "REFUNDED";
      const shpOk = row.ship_status === "NONE" || row.ship_status === "CANCELLED";
      if (!invOk || !payOk || !shpOk) {
        inconsistentCount++;
      }
    } else {
      inconsistentCount++;
    }
  }

  let stockValid = true;
  for (const st of stockRows) {
    if (Number(st.available_qty) + Number(st.reserved_qty) !== Number(st.initial_qty)) {
      stockValid = false;
    }
  }

  const cbs = (
    await env.DB.prepare("SELECT * FROM circuit_breakers").all()
  ).results;

  const totalSagas = Number(sagaCounts?.total || 0);
  const completedSagas = Number(sagaCounts?.completed || 0);
  const inconsistentPct =
    totalSagas > 0 ? ((inconsistentCount / totalSagas) * 100).toFixed(4) : "0.0000";

  // Reconciler state
  const batchTotal = await env.DB.prepare(
    "SELECT COUNT(*) AS cnt FROM raw_events_batch"
  ).first();

  const streamWinRows = (
    await env.DB.prepare(
      "SELECT * FROM streaming_windows ORDER BY window_id ASC"
    ).all()
  ).results;

  const batchWinRows = (
    await env.DB.prepare(
      `SELECT window_id, COUNT(*) AS cnt, SUM(amount_cents) AS sum_amt, COUNT(DISTINCT user_id) AS exact_users
       FROM raw_events_batch
       GROUP BY window_id
       ORDER BY window_id ASC`
    ).all()
  ).results;

  const batchByWin = new Map();
  for (const b of batchWinRows) {
    batchByWin.set(Number(b.window_id), b);
  }

  const windowsView = streamWinRows.map((sw) => {
    const wid = Number(sw.window_id);
    const b = batchByWin.get(wid) || { cnt: 0, sum_amt: 0, exact_users: 0 };
    const hllRegs = deserializeHLL(sw.hll_registers_b64);
    const hllEst =
      sw.exact_users_override !== null && sw.exact_users_override !== undefined
        ? Number(sw.exact_users_override)
        : hllCount(hllRegs);
    return {
      window_id: wid,
      stream_count: Number(sw.event_count),
      batch_count: Number(b.cnt),
      stream_sum: Number(sw.amount_sum_cents),
      batch_sum: Number(b.sum_amt || 0),
      hll_users: hllEst,
      batch_users: Number(b.exact_users || 0),
      diagnosed_anomaly: sw.diagnosed_anomaly,
      auto_corrected: Number(sw.auto_corrected) === 1,
    };
  });

  const lastRecRun = await env.DB.prepare(
    "SELECT * FROM reconciliation_runs ORDER BY id DESC LIMIT 1"
  ).first();

  // MapReduce state + check Mac Coordinator health
  const mrJobs = (
    await env.DB.prepare(
      "SELECT * FROM mapreduce_jobs ORDER BY created_at_ms DESC LIMIT 10"
    ).all()
  ).results;

  let clusterOnline = false;
  let workerNodes = [];
  try {
    const resp = await fetch(`${goCoordinatorUrl}/health`, {
      signal: AbortSignal.timeout(400),
    });
    if (resp.ok) {
      const h = await resp.json();
      clusterOnline = true;
      workerNodes = h.workers || [];
    }
  } catch (_) {
    clusterOnline = false;
  }

  return {
    saga: {
      total: totalSagas,
      completed: completedSagas,
      compensated: Number(sagaCounts?.compensated || 0),
      dlq_pending: Number(dlqPending?.cnt || 0),
      success_rate_pct:
        totalSagas > 0 ? ((completedSagas / totalSagas) * 100).toFixed(1) : "0.0",
      zombies_fenced: Number(sagaCounts?.zombies_fenced || 0),
      avg_rollback_ms: (Number(sagaCounts?.avg_rollback_us || 0) / 1000).toFixed(3),
      inconsistent_pct: inconsistentPct,
      stock_valid: stockValid,
      circuit_breakers: cbs,
      recent: sagaRows,
    },
    reconciler: {
      total_batch_events: Number(batchTotal?.cnt || 0),
      windows: windowsView,
      last_run: lastRecRun || null,
    },
    mapreduce: {
      cluster_online: clusterOnline,
      worker_nodes: workerNodes,
      jobs: mrJobs,
    },
  };
}

async function executeSagaBatch(env, body) {
  const count = Math.min(200, Math.max(1, Number(body.count || 20)));
  const failRate = Number(body.fail_rate ?? 0.2);
  const zombieRate = Number(body.zombie_rate ?? 0.15);
  const forceDlq = Boolean(body.force_dlq);

  const results = [];
  for (let i = 0; i < count; i++) {
    const sagaId = `saga-${Date.now().toString(36)}-${i}`;
    const sku = SKUS[i % SKUS.length];
    const qty = (i % 3) + 1;
    const amountCents = qty * 2500;
    const nowMs = Date.now();

    await env.DB.prepare(
      `INSERT INTO sagas (saga_id, status, version_epoch, sku, qty, amount_cents, created_at_ms)
       VALUES (?, 'RUNNING', 1, ?, ?, ?, ?)`
    )
      .bind(sagaId, sku, qty, amountCents, nowMs)
      .run();

    await env.DB.prepare(
      `INSERT INTO service_ledger (saga_id, sku, qty, amount_cents, inv_status, pay_status, ship_status)
       VALUES (?, ?, ?, ?, 'NONE', 'NONE', 'NONE')`
    )
      .bind(sagaId, sku, qty, amountCents)
      .run();

    const isZombieTimeout = Math.random() < zombieRate;
    if (isZombieTimeout) {
      // Step 1 times out at orchestrator -> Orchestrator immediately plants a Tombstone in D1
      await env.DB.prepare(
        `INSERT OR REPLACE INTO service_tombstones (saga_id, service_name, fenced_hits, created_at_ms)
         VALUES (?, 'reserve_inventory', 0, ?)`
      )
        .bind(sagaId, nowMs)
        .run();

      // Now the delayed zombie forward RPC arrives at InventoryService and checks D1 tombstone!
      const tomb = await env.DB.prepare(
        "SELECT saga_id FROM service_tombstones WHERE saga_id = ?"
      )
        .bind(sagaId)
        .first();

      let zombieFenced = 0;
      if (tomb) {
        zombieFenced = 1;
        await env.DB.prepare(
          "UPDATE service_tombstones SET fenced_hits = fenced_hits + 1 WHERE saga_id = ?"
        )
          .bind(sagaId)
          .run();
      }

      const rbUs = 45 + Math.random() * 80;
      await env.DB.prepare(
        `UPDATE sagas
         SET status = 'COMPENSATED', version_epoch = version_epoch + 1,
             failed_step = 'reserve_inventory (timeout)',
             error = 'Zombie forward RPC fenced by D1 tombstone',
             detect_latency_us = 8.4, rollback_latency_us = ?, zombie_fenced = ?
         WHERE saga_id = ?`
      )
        .bind(rbUs, zombieFenced, sagaId)
        .run();

      results.push({ saga_id: sagaId, status: "COMPENSATED", zombie_fenced: true });
      continue;
    }

    // Step 1: Reserve Inventory in D1
    await env.DB.prepare(
      `UPDATE inventory_stock
       SET available_qty = available_qty - ?, reserved_qty = reserved_qty + ?
       WHERE sku = ?`
    )
      .bind(qty, qty, sku)
      .run();

    await env.DB.prepare(
      "UPDATE service_ledger SET inv_status = 'RESERVED' WHERE saga_id = ?"
    )
      .bind(sagaId)
      .run();

    // Step 2: Charge Payment
    const payFailed = Math.random() < failRate;
    if (payFailed) {
      await env.DB.prepare(
        "UPDATE circuit_breakers SET consecutive_failures = consecutive_failures + 1 WHERE service_name = 'charge_payment'"
      ).run();

      if (forceDlq) {
        // Simulate compensation outage -> route to Dead-Letter Queue in D1!
        await env.DB.prepare(
          `INSERT INTO dead_letter_queue (saga_id, step_name, sku, qty, amount_cents, last_error, attempts, resolved)
           VALUES (?, 'release_inventory', ?, ?, ?, 'Inventory DB lock timeout during rollback', 5, 0)`
        )
          .bind(sagaId, sku, qty, amountCents)
          .run();

        await env.DB.prepare(
          `UPDATE sagas
           SET status = 'FAILED_DLQ', failed_step = 'charge_payment',
               error = 'Compensation exhausted retries -> Enqueued in DLQ'
           WHERE saga_id = ?`
        )
          .bind(sagaId)
          .run();
        results.push({ saga_id: sagaId, status: "FAILED_DLQ" });
        continue;
      }

      // Normal Reverse Compensation: release inventory in D1
      const rbUs = 95 + Math.random() * 140;
      await env.DB.prepare(
        `UPDATE inventory_stock
         SET available_qty = available_qty + ?, reserved_qty = reserved_qty - ?
         WHERE sku = ?`
      )
        .bind(qty, qty, sku)
        .run();

      await env.DB.prepare(
        "UPDATE service_ledger SET inv_status = 'RELEASED' WHERE saga_id = ?"
      )
        .bind(sagaId)
        .run();

      await env.DB.prepare(
        `UPDATE sagas
         SET status = 'COMPENSATED', version_epoch = version_epoch + 1,
             failed_step = 'charge_payment', error = 'Payment gateway declined',
             detect_latency_us = 7.9, rollback_latency_us = ?
         WHERE saga_id = ?`
      )
        .bind(rbUs, sagaId)
        .run();

      results.push({ saga_id: sagaId, status: "COMPENSATED" });
      continue;
    }

    await env.DB.prepare(
      "UPDATE service_ledger SET pay_status = 'CHARGED' WHERE saga_id = ?"
    )
      .bind(sagaId)
      .run();

    // Step 3: Create Shipment
    const shipFailed = Math.random() < failRate;
    if (shipFailed) {
      // Reverse Compensation: 1) Refund Payment, 2) Release Inventory
      const rbUs = 160 + Math.random() * 180;
      await env.DB.prepare(
        "UPDATE service_ledger SET pay_status = 'REFUNDED', inv_status = 'RELEASED' WHERE saga_id = ?"
      )
        .bind(sagaId)
        .run();

      await env.DB.prepare(
        `UPDATE inventory_stock
         SET available_qty = available_qty + ?, reserved_qty = reserved_qty - ?
         WHERE sku = ?`
      )
        .bind(qty, qty, sku)
        .run();

      await env.DB.prepare(
        `UPDATE sagas
         SET status = 'COMPENSATED', version_epoch = version_epoch + 1,
             failed_step = 'create_shipment', error = 'Carrier dispatch timeout',
             detect_latency_us = 8.2, rollback_latency_us = ?
         WHERE saga_id = ?`
      )
        .bind(rbUs, sagaId)
        .run();

      results.push({ saga_id: sagaId, status: "COMPENSATED" });
      continue;
    }

    await env.DB.prepare(
      "UPDATE service_ledger SET ship_status = 'SCHEDULED' WHERE saga_id = ?"
    )
      .bind(sagaId)
      .run();

    await env.DB.prepare(
      "UPDATE sagas SET status = 'COMPLETED', version_epoch = version_epoch + 1 WHERE saga_id = ?"
    )
      .bind(sagaId)
      .run();

    results.push({ saga_id: sagaId, status: "COMPLETED" });
  }

  return { executed: count, results };
}

async function replayDLQ(env) {
  const pending = (
    await env.DB.prepare(
      "SELECT * FROM dead_letter_queue WHERE resolved = 0"
    ).all()
  ).results;

  let replayed = 0;
  for (const item of pending) {
    await env.DB.prepare(
      `UPDATE inventory_stock
       SET available_qty = available_qty + ?, reserved_qty = reserved_qty - ?
       WHERE sku = ?`
    )
      .bind(Number(item.qty), Number(item.qty), item.sku)
      .run();

    await env.DB.prepare(
      "UPDATE service_ledger SET inv_status = 'RELEASED' WHERE saga_id = ?"
    )
      .bind(item.saga_id)
      .run();

    await env.DB.prepare(
      `UPDATE sagas
       SET status = 'COMPENSATED', error = 'Recovered via DLQ Replay', rollback_latency_us = 210.0
       WHERE saga_id = ?`
    )
      .bind(item.saga_id)
      .run();

    await env.DB.prepare(
      "UPDATE dead_letter_queue SET resolved = 1, attempts = attempts + 1 WHERE id = ?"
    )
      .bind(item.id)
      .run();

    replayed++;
  }
  return { replayed };
}

async function ingestStreamBatch(env, body) {
  const numEvents = Math.min(2000, Math.max(20, Number(body.events || 400)));
  const mode = body.mode || "ALTERNATING";
  const dropRate = Number(body.drop_rate ?? 0.06);
  const dupRate = Number(body.duplicate_rate ?? 0.06);
  const numWindows = 10;

  const windowCache = new Map();
  const existingRows = (
    await env.DB.prepare("SELECT * FROM streaming_windows").all()
  ).results;
  for (const r of existingRows) {
    windowCache.set(Number(r.window_id), {
      window_id: Number(r.window_id),
      event_count: Number(r.event_count),
      amount_sum_cents: Number(r.amount_sum_cents),
      hll: deserializeHLL(r.hll_registers_b64),
      dropped_injected: Number(r.dropped_injected),
      dup_injected: Number(r.dup_injected),
      late_injected: Number(r.late_injected),
    });
  }

  const baseId = Date.now();
  let dropped = 0;
  let duplicated = 0;

  for (let i = 0; i < numEvents; i++) {
    const windowId = i % numWindows;
    const eventId = `evt-${baseId}-${i}`;
    const userId = `user-${(i * 7) % 350}`;
    const amount = 100 + ((i * 31) % 900);
    const evTime = windowId * 10.0 + (i % 10);

    // 1. Always write to Batch Source-of-Truth table in D1
    await env.DB.prepare(
      `INSERT OR IGNORE INTO raw_events_batch (event_id, window_id, user_id, amount_cents, event_time_sec)
       VALUES (?, ?, ?, ?, ?)`
    )
      .bind(eventId, windowId, userId, amount, evTime)
      .run();

    // 2. Streaming Path with Fault Injection + 1KB HyperLogLog Sketch
    let w = windowCache.get(windowId);
    if (!w) {
      w = {
        window_id: windowId,
        event_count: 0,
        amount_sum_cents: 0,
        hll: createEmptyHLL(),
        dropped_injected: 0,
        dup_injected: 0,
        late_injected: 0,
      };
      windowCache.set(windowId, w);
    }

    let injectDrop = false;
    let injectDup = false;
    if (mode === "ALTERNATING") {
      if (windowId % 2 === 1) {
        if (windowId % 4 === 1) injectDup = Math.random() < dupRate;
        else injectDrop = Math.random() < dropRate;
      }
    } else if (mode === "DUP_ONLY") {
      injectDup = windowId % 2 === 1 && Math.random() < dupRate;
    } else if (mode === "DROP_ONLY") {
      injectDrop = windowId % 2 === 1 && Math.random() < dropRate;
    }

    if (injectDrop) {
      w.dropped_injected++;
      dropped++;
      continue;
    }

    w.event_count++;
    w.amount_sum_cents += amount;
    hllAdd(w.hll, userId);

    if (injectDup) {
      w.event_count++;
      w.amount_sum_cents += amount;
      hllAdd(w.hll, userId);
      w.dup_injected++;
      duplicated++;
    }
  }

  for (const w of windowCache.values()) {
    await env.DB.prepare(
      `INSERT OR REPLACE INTO streaming_windows
       (window_id, event_count, amount_sum_cents, hll_registers_b64, exact_users_override,
        dropped_injected, dup_injected, late_injected, diagnosed_anomaly, auto_corrected)
       VALUES (?, ?, ?, ?, NULL, ?, ?, ?, 'NONE', 0)`
    )
      .bind(
        w.window_id,
        w.event_count,
        w.amount_sum_cents,
        serializeHLL(w.hll),
        w.dropped_injected,
        w.dup_injected,
        w.late_injected
      )
      .run();
  }

  return { ingested: numEvents, dropped, duplicated };
}

async function reconcileWindows(env, body) {
  const startMs = performance.now();
  const autoCorrect = body.auto_correct !== false;
  const thresholdPct = Number(body.threshold_pct ?? 1.0);

  const streamRows = (
    await env.DB.prepare(
      "SELECT * FROM streaming_windows ORDER BY window_id ASC"
    ).all()
  ).results;

  const batchRows = (
    await env.DB.prepare(
      `SELECT window_id, COUNT(*) AS cnt, SUM(amount_cents) AS sum_amt, COUNT(DISTINCT user_id) AS exact_users
       FROM raw_events_batch
       GROUP BY window_id
       ORDER BY window_id ASC`
    ).all()
  ).results;

  const batchMap = new Map();
  for (const b of batchRows) {
    batchMap.set(Number(b.window_id), b);
  }

  let flaggedCount = 0;
  let accurateRootCauses = 0;
  const driftsBefore = [];
  const driftsAfter = [];
  const details = [];

  for (const sw of streamRows) {
    const wid = Number(sw.window_id);
    const b = batchMap.get(wid) || { cnt: 0, sum_amt: 0, exact_users: 0 };
    const sCount = Number(sw.event_count);
    const bCount = Number(b.cnt);
    const sSum = Number(sw.amount_sum_cents);
    const bSum = Number(b.sum_amt || 0);
    const bUsers = Number(b.exact_users || 0);

    const cDiff = bCount > 0 ? (Math.abs(sCount - bCount) / bCount) * 100 : 0;
    const sDiff = bSum > 0 ? (Math.abs(sSum - bSum) / bSum) * 100 : 0;
    const maxDiff = Math.max(cDiff, sDiff);
    driftsBefore.push(maxDiff);

    const flagged = maxDiff > thresholdPct;
    let diagnosed = "NONE";
    if (flagged) {
      flaggedCount++;
      if (sCount > bCount) {
        diagnosed = "DUPLICATE_DELIVERY";
      } else if (sCount < bCount) {
        diagnosed = "MESSAGE_LOSS_OR_LATE";
      } else {
        diagnosed = "MIXED_DRIFT";
      }

      const expected =
        Number(sw.dup_injected) > 0 && Number(sw.dropped_injected) === 0
          ? "DUPLICATE_DELIVERY"
          : Number(sw.dropped_injected) > 0 && Number(sw.dup_injected) === 0
          ? "MESSAGE_LOSS_OR_LATE"
          : "MIXED_DRIFT";

      if (diagnosed === expected) {
        accurateRootCauses++;
      }

      if (autoCorrect) {
        await env.DB.prepare(
          `UPDATE streaming_windows
           SET event_count = ?, amount_sum_cents = ?, exact_users_override = ?,
               diagnosed_anomaly = ?, auto_corrected = 1
           WHERE window_id = ?`
        )
          .bind(bCount, bSum, bUsers, diagnosed, wid)
          .run();
        driftsAfter.push(0.0);
      } else {
        await env.DB.prepare(
          "UPDATE streaming_windows SET diagnosed_anomaly = ? WHERE window_id = ?"
        )
          .bind(diagnosed, wid)
          .run();
        driftsAfter.push(maxDiff);
      }
    } else {
      driftsAfter.push(0.0);
    }

    details.push({
      window_id: wid,
      drift_before_pct: Number(maxDiff.toFixed(2)),
      flagged,
      diagnosed,
    });
  }

  const latencyMs = Math.max(0.1, performance.now() - startMs);
  const meanBefore =
    driftsBefore.length > 0
      ? driftsBefore.reduce((a, b) => a + b, 0) / driftsBefore.length
      : 0;
  const meanAfter =
    driftsAfter.length > 0
      ? driftsAfter.reduce((a, b) => a + b, 0) / driftsAfter.length
      : 0;
  const rcAcc =
    flaggedCount > 0 ? (accurateRootCauses / flaggedCount) * 100 : 100.0;

  await env.DB.prepare(
    `INSERT INTO reconciliation_runs
     (run_at_ms, reconcile_latency_ms, total_windows, flagged_windows,
      mean_drift_before_pct, mean_drift_after_pct, root_cause_accuracy_pct, details_json)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(
      Date.now(),
      Number(latencyMs.toFixed(3)),
      streamRows.length,
      flaggedCount,
      Number(meanBefore.toFixed(3)),
      Number(meanAfter.toFixed(3)),
      Number(rcAcc.toFixed(1)),
      JSON.stringify(details)
    )
    .run();

  return {
    total_windows: streamRows.length,
    flagged_windows: flaggedCount,
    reconcile_latency_ms: Number(latencyMs.toFixed(3)),
    mean_drift_before_pct: Number(meanBefore.toFixed(3)),
    mean_drift_after_pct: Number(meanAfter.toFixed(3)),
    root_cause_accuracy_pct: Number(rcAcc.toFixed(1)),
  };
}

async function dispatchMapReduceJob(env, goCoordinatorUrl, body) {
  const records = Number(body.records || 200000);
  const speculative = body.speculative !== false;
  const injectCrash = Boolean(body.inject_crash);
  const injectStraggler = Boolean(body.inject_straggler);

  let jobResult = null;
  let mode = "Multi-Node Go HTTP Cluster";

  try {
    const resp = await fetch(`${goCoordinatorUrl}/api/job`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        records,
        speculative,
        inject_crash: injectCrash,
        inject_straggler: injectStraggler,
      }),
      signal: AbortSignal.timeout(8000),
    });
    if (resp.ok) {
      jobResult = await resp.json();
    }
  } catch (_) {
    // Fallback if Mac tunnel is offline
  }

  if (!jobResult) {
    mode = "Edge Isolate Simulation";
    const baseWall = injectStraggler && !speculative ? 96.4 : 24.8;
    jobResult = {
      job_id: `mr-${Date.now().toString(36)}`,
      scenario: "Edge Fallback Run",
      records,
      chunks: 24,
      workers: 4,
      wall_time_ms: baseWall,
      throughput_rec_sec: Math.round((records / baseWall) * 1000),
      speedup: injectStraggler && !speculative ? 1.48 : 4.92,
      failures_recovered: injectCrash ? 2 : 0,
      speculative_wins: injectStraggler && speculative ? 2 : 0,
      data_loss_pct: 0.0,
      endpoints: [],
    };
  }

  await env.DB.prepare(
    `INSERT INTO mapreduce_jobs
     (job_id, scenario, records, chunks, workers, wall_time_ms, throughput_rec_sec,
      speedup, failures_recovered, speculative_wins, data_loss_pct, mode, summary_json, created_at_ms)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(
      jobResult.job_id,
      jobResult.scenario || mode,
      jobResult.records,
      jobResult.chunks,
      jobResult.workers,
      jobResult.wall_time_ms,
      jobResult.throughput_rec_sec,
      jobResult.speedup,
      jobResult.failures_recovered,
      jobResult.speculative_wins,
      jobResult.data_loss_pct,
      mode,
      JSON.stringify(jobResult.endpoints || []),
      Date.now()
    )
    .run();

  return jobResult;
}

async function resetDatabase(env) {
  const tables = [
    "sagas",
    "saga_step_events",
    "service_ledger",
    "service_tombstones",
    "dead_letter_queue",
    "raw_events_batch",
    "streaming_windows",
    "reconciliation_runs",
    "mapreduce_jobs",
  ];
  for (const t of tables) {
    await env.DB.prepare(`DELETE FROM ${t}`).run();
  }
  await env.DB.prepare(
    "UPDATE inventory_stock SET available_qty = initial_qty, reserved_qty = 0"
  ).run();
  await env.DB.prepare(
    "UPDATE circuit_breakers SET state = 'CLOSED', consecutive_failures = 0, fast_rejects = 0"
  ).run();
  return { reset: true };
}

export default {
  async fetch(request, env, ctx) {
    if (request.method === "OPTIONS") {
      return jsonResponse({ ok: true });
    }

    const url = new URL(request.url);
    const goCoordinatorUrl =
      env.GO_CLUSTER_COORDINATOR_URL || "http://127.0.0.1:9090";

    if (url.pathname === "/" && request.method === "GET") {
      return new Response(renderDashboardHTML(), {
        headers: { "Content-Type": "text/html; charset=utf-8" },
      });
    }

    if (url.pathname === "/api/state" && request.method === "GET") {
      return jsonResponse(await getSystemState(env, goCoordinatorUrl));
    }

    if (url.pathname === "/api/saga/execute" && request.method === "POST") {
      const body = await request.json();
      return jsonResponse(await executeSagaBatch(env, body));
    }

    if (url.pathname === "/api/saga/dlq/replay" && request.method === "POST") {
      return jsonResponse(await replayDLQ(env));
    }

    if (url.pathname === "/api/reconciler/ingest" && request.method === "POST") {
      const body = await request.json();
      return jsonResponse(await ingestStreamBatch(env, body));
    }

    if (
      url.pathname === "/api/reconciler/reconcile" &&
      request.method === "POST"
    ) {
      const body = await request.json();
      return jsonResponse(await reconcileWindows(env, body));
    }

    if (url.pathname === "/api/mapreduce/run" && request.method === "POST") {
      const body = await request.json();
      return jsonResponse(
        await dispatchMapReduceJob(env, goCoordinatorUrl, body)
      );
    }

    if (url.pathname === "/api/reset" && request.method === "POST") {
      return jsonResponse(await resetDatabase(env));
    }

    return jsonResponse({ error: "Not found", path: url.pathname }, 404);
  },
};
