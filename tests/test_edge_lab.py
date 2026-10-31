from __future__ import annotations

import os
import subprocess
import time
import unittest
from pathlib import Path

from mac_cli import api_call


class CloudflareEdgeLabTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parent.parent
        cls.port = 18787
        cls.base_url = f"http://127.0.0.1:{cls.port}"
        env = dict(os.environ)
        env["PORT"] = str(cls.port)
        cls.proc = subprocess.Popen(
            ["node", "server.mjs"],
            cwd=str(cls.root),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for _ in range(30):
            try:
                api_call(cls.base_url, "/api/state")
                break
            except Exception:
                time.sleep(0.1)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.proc.terminate()
        cls.proc.wait(timeout=5)

    def test_end_to_end_edge_saga_reconciler_and_mapreduce(self) -> None:
        api_call(self.base_url, "/api/reset", "POST", {})

        # 1. Execute sagas with DLQ outage, verify DLQ captures failed rollbacks, then replay DLQ
        api_call(
            self.base_url,
            "/api/saga/execute",
            "POST",
            {"count": 30, "fail_rate": 0.30, "zombie_rate": 0.20, "force_dlq": True},
        )
        state_mid = api_call(self.base_url, "/api/state")
        self.assertEqual(state_mid["saga"]["total"], 30)
        self.assertTrue(state_mid["saga"]["stock_valid"])

        api_call(self.base_url, "/api/saga/dlq/replay", "POST", {})
        state_after_dlq = api_call(self.base_url, "/api/state")
        self.assertEqual(state_after_dlq["saga"]["dlq_pending"], 0)
        self.assertEqual(state_after_dlq["saga"]["inconsistent_pct"], "0.0000")
        self.assertTrue(state_after_dlq["saga"]["stock_valid"])

        # 2. Stream events into 1KB D1 HyperLogLog sketches + Batch table and reconcile
        api_call(
            self.base_url,
            "/api/reconciler/ingest",
            "POST",
            {"events": 500, "mode": "ALTERNATING", "drop_rate": 0.10, "duplicate_rate": 0.10},
        )
        rec_res = api_call(
            self.base_url,
            "/api/reconciler/reconcile",
            "POST",
            {"auto_correct": True, "threshold_pct": 0.5},
        )
        self.assertEqual(rec_res["mean_drift_after_pct"], 0.0)
        self.assertEqual(rec_res["root_cause_accuracy_pct"], 100.0)

        # 3. Run MapReduce job
        mr_res = api_call(
            self.base_url,
            "/api/mapreduce/run",
            "POST",
            {"records": 100000, "speculative": True, "inject_crash": True, "inject_straggler": True},
        )
        self.assertEqual(mr_res["data_loss_pct"], 0.0)


if __name__ == "__main__":
    unittest.main()
