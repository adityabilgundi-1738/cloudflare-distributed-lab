from __future__ import annotations

import argparse
import json
import urllib.request
from typing import Any, Dict


def api_call(base_url: str, path: str, method: str = "GET", payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    url = base_url.rstrip("/") + path
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json", "User-Agent": "Mac-Chaos-CLI/1.0"},
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def print_status(state: Dict[str, Any]) -> None:
    s = state["saga"]
    r = state["reconciler"]
    m = state["mapreduce"]
    print("=" * 100)
    print("CLOUDFLARE EDGE WORKER + D1 SQLITE — LIVE DISTRIBUTED TELEMETRY")
    print("=" * 100)
    print(
        f"[1] Edge Saga Coordinator : {s['total']} total | {s['completed']} completed | "
        f"{s['compensated']} compensated | DLQ pending: {s['dlq_pending']}"
    )
    print(
        f"    Consistency & Fencing : {s['zombies_fenced']} Zombie RPCs fenced by D1 Tombstones | "
        f"Inconsistent: {s['inconsistent_pct']}% | Stock Valid: {s['stock_valid']}"
    )
    print("-" * 100)
    last_run = r.get("last_run") or {}
    print(
        f"[2] Stream/Batch Engine   : {r['total_batch_events']} batch events | "
        f"{len(r['windows'])} active windows (1 KB HLL sketch per window)"
    )
    if last_run:
        print(
            f"    Last Reconciliation   : {last_run['flagged_windows']} flagged | "
            f"Drift {last_run['mean_drift_before_pct']}% -> {last_run['mean_drift_after_pct']}% | "
            f"Root-Cause Acc: {last_run['root_cause_accuracy_pct']}% | Latency: {last_run['reconcile_latency_ms']} ms"
        )
    print("-" * 100)
    print(
        f"[3] Hybrid Go MapReduce   : Mac Cluster Online = {m['cluster_online']} "
        f"({len(m['worker_nodes'])} worker nodes) | Jobs Recorded = {len(m['jobs'])}"
    )
    if m["jobs"]:
        j = m["jobs"][0]
        print(
            f"    Latest Job ({j['job_id']}) : {j['throughput_rec_sec']:,.0f} rec/s | "
            f"Wall: {j['wall_time_ms']} ms ({j['speedup']}x speedup) | "
            f"Failures Fixed: {j['failures_recovered']} | Speculative Wins: {j['speculative_wins']} | "
            f"Data Loss: {j['data_loss_pct']}%"
        )
    print("=" * 100)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mac CLI Controller for Cloudflare Worker + D1 Distributed Systems Lab"
    )
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:8787",
        help="Target Cloudflare Worker URL (local :8787, *.workers.dev, or *.trycloudflare.com)",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("status", help="Fetch live D1 state across all 3 distributed engines")
    sub.add_parser("reset", help="Reset all D1 tables to a clean initial state")
    sub.add_parser("dlq-replay", help="Replay all pending Dead-Letter Queue compensations in D1")

    p_saga = sub.add_parser("saga", help="Execute a batch of sagas with fault & zombie injection")
    p_saga.add_argument("--count", type=int, default=30)
    p_saga.add_argument("--fail-rate", type=float, default=0.20)
    p_saga.add_argument("--zombie-rate", type=float, default=0.15)
    p_saga.add_argument("--force-dlq", action="store_true")

    p_stream = sub.add_parser("stream", help="Stream events to Edge HLL + D1 Batch store")
    p_stream.add_argument("--events", type=int, default=600)
    p_stream.add_argument("--mode", default="ALTERNATING", choices=["ALTERNATING", "DUP_ONLY", "DROP_ONLY"])
    p_stream.add_argument("--drop-rate", type=float, default=0.06)
    p_stream.add_argument("--duplicate-rate", type=float, default=0.06)
    p_stream.add_argument("--reconcile", action="store_true", help="Immediately run reconciliation after ingest")

    p_mr = sub.add_parser("mapreduce", help="Dispatch a MapReduce job to the Go worker cluster")
    p_mr.add_argument("--records", type=int, default=200000)
    p_mr.add_argument("--speculative", action="store_true", default=True)
    p_mr.add_argument("--no-speculative", action="store_false", dest="speculative")
    p_mr.add_argument("--inject-crash", action="store_true", default=True)
    p_mr.add_argument("--inject-straggler", action="store_true", default=True)

    sub.add_parser("demo", help="Run an end-to-end chaos showcase across all 3 systems")

    args = parser.parse_args()

    if args.cmd == "status":
        print_status(api_call(args.url, "/api/state"))
    elif args.cmd == "reset":
        api_call(args.url, "/api/reset", "POST", {})
        print_status(api_call(args.url, "/api/state"))
    elif args.cmd == "dlq-replay":
        res = api_call(args.url, "/api/saga/dlq/replay", "POST", {})
        print(f"Replayed {res['replayed']} DLQ entries.")
        print_status(api_call(args.url, "/api/state"))
    elif args.cmd == "saga":
        api_call(
            args.url,
            "/api/saga/execute",
            "POST",
            {
                "count": args.count,
                "fail_rate": args.fail_rate,
                "zombie_rate": args.zombie_rate,
                "force_dlq": args.force_dlq,
            },
        )
        print_status(api_call(args.url, "/api/state"))
    elif args.cmd == "stream":
        api_call(
            args.url,
            "/api/reconciler/ingest",
            "POST",
            {
                "events": args.events,
                "mode": args.mode,
                "drop_rate": args.drop_rate,
                "duplicate_rate": args.duplicate_rate,
            },
        )
        if args.reconcile:
            api_call(args.url, "/api/reconciler/reconcile", "POST", {"auto_correct": True, "threshold_pct": 1.0})
        print_status(api_call(args.url, "/api/state"))
    elif args.cmd == "mapreduce":
        api_call(
            args.url,
            "/api/mapreduce/run",
            "POST",
            {
                "records": args.records,
                "speculative": args.speculative,
                "inject_crash": args.inject_crash,
                "inject_straggler": args.inject_straggler,
            },
        )
        print_status(api_call(args.url, "/api/state"))
    elif args.cmd == "demo":
        api_call(args.url, "/api/reset", "POST", {})
        api_call(
            args.url,
            "/api/saga/execute",
            "POST",
            {"count": 40, "fail_rate": 0.25, "zombie_rate": 0.20, "force_dlq": True},
        )
        api_call(args.url, "/api/saga/dlq/replay", "POST", {})
        api_call(
            args.url,
            "/api/reconciler/ingest",
            "POST",
            {"events": 800, "mode": "ALTERNATING", "drop_rate": 0.08, "duplicate_rate": 0.08},
        )
        api_call(args.url, "/api/reconciler/reconcile", "POST", {"auto_correct": True, "threshold_pct": 1.0})
        api_call(
            args.url,
            "/api/mapreduce/run",
            "POST",
            {"records": 200000, "speculative": True, "inject_crash": True, "inject_straggler": True},
        )
        print_status(api_call(args.url, "/api/state"))


if __name__ == "__main__":
    main()
