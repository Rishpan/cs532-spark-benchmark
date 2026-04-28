"""Merge benchmark result payloads with idempotent deduplication keys."""
from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_text(path: str) -> str | None:
    if path.startswith("gs://"):
        with tempfile.NamedTemporaryFile(mode="r", suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            proc = subprocess.run(
                ["gcloud", "storage", "cp", path, tmp_path],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                return None
            return Path(tmp_path).read_text()
        finally:
            Path(tmp_path).unlink(missing_ok=True)
    p = Path(path)
    if not p.exists():
        return None
    return p.read_text()


def _write_text(path: str, content: str) -> None:
    if path.startswith("gs://"):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name
        try:
            subprocess.run(["gcloud", "storage", "cp", tmp_path, path], check=True)
        finally:
            Path(tmp_path).unlink(missing_ok=True)
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content)


def _payload_records_wall_clock(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    benchmark_id = payload.get("benchmark_id")
    scale_pct = payload.get("scale_pct")
    run_records: list[dict[str, Any]] = []
    for query in (
        "error_pattern_analysis",
        "temporal_aggregation",
        "perhost_traffic_profiling",
        "perhost_traffic_profiling_naive",
        "sessionization",
    ):
        for rec in payload.get(query, []):
            run_records.append(
                {
                    "benchmark_id": benchmark_id,
                    "scale_pct": scale_pct,
                    "query": query,
                    "api": rec["api"],
                    "run": rec["run"],
                    "elapsed_sec": rec["elapsed_sec"],
                }
            )

    summary_records: list[dict[str, Any]] = []
    summary = payload.get("summary", {})
    for query, by_api in summary.items():
        if isinstance(by_api, dict):
            for api, stats in by_api.items():
                summary_records.append(
                    {
                        "benchmark_id": benchmark_id,
                        "scale_pct": scale_pct,
                        "query": query,
                        "api": api,
                        "num_runs": stats.get("num_runs"),
                        "avg_elapsed_sec": stats.get("avg_elapsed_sec"),
                        "std_elapsed_sec": stats.get("std_elapsed_sec"),
                    }
                )
    return run_records, summary_records


def _payload_records_stage_metrics(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    benchmark_id = payload.get("benchmark_id")
    scale_pct = payload.get("scale_pct")
    run_records: list[dict[str, Any]] = []
    for query in (
        "error_pattern_analysis",
        "temporal_aggregation",
        "perhost_traffic_profiling",
        "perhost_traffic_profiling_naive",
        "sessionization",
    ):
        for rec in payload.get(query, []):
            run_records.append(
                {
                    "benchmark_id": benchmark_id,
                    "scale_pct": scale_pct,
                    "query": query,
                    "api": rec["api"],
                    "run": rec["run"],
                    "num_stages": rec["num_stages"],
                    "num_tasks": rec["num_tasks"],
                    "shuffle_read_bytes": rec["shuffle_read_bytes"],
                    "shuffle_write_bytes": rec["shuffle_write_bytes"],
                    "disk_bytes_spilled": rec["disk_bytes_spilled"],
                    "executor_deserialize_time_sec": rec["executor_deserialize_time_sec"],
                    "executor_cpu_time_sec": rec["executor_cpu_time_sec"],
                }
            )

    summary_records: list[dict[str, Any]] = []
    summary = payload.get("summary", {})
    for query, by_api in summary.items():
        if isinstance(by_api, dict):
            for api, stats in by_api.items():
                summary_records.append(
                    {
                        "benchmark_id": benchmark_id,
                        "scale_pct": scale_pct,
                        "query": query,
                        "api": api,
                        **stats,
                    }
                )
    return run_records, summary_records


def _run_key(rec: dict[str, Any]) -> tuple[Any, ...]:
    return (rec["benchmark_id"], rec["scale_pct"], rec["query"], rec["api"], rec["run"])


def _summary_key(rec: dict[str, Any]) -> tuple[Any, ...]:
    return (rec["benchmark_id"], rec["scale_pct"], rec["query"], rec["api"])


def merge_payload(kind: str, payload: dict[str, Any], merged_output_path: str) -> None:
    existing_text = _read_text(merged_output_path)
    if existing_text is None:
        merged: dict[str, Any] = {
            "kind": kind,
            "updated_at_utc": _utc_now(),
            "records": [],
            "summary_records": [],
        }
    else:
        merged = json.loads(existing_text)
        merged.setdefault("records", [])
        merged.setdefault("summary_records", [])

    if kind == "wall_clock":
        records, summary_records = _payload_records_wall_clock(payload)
    elif kind == "stage_metrics":
        records, summary_records = _payload_records_stage_metrics(payload)
    else:
        raise ValueError(f"Unsupported merge kind: {kind}")

    by_run = {_run_key(rec): rec for rec in merged["records"]}
    for rec in records:
        by_run[_run_key(rec)] = rec
    merged["records"] = list(by_run.values())

    by_summary = {_summary_key(rec): rec for rec in merged["summary_records"]}
    for rec in summary_records:
        by_summary[_summary_key(rec)] = rec
    merged["summary_records"] = list(by_summary.values())
    merged["updated_at_utc"] = _utc_now()

    _write_text(merged_output_path, json.dumps(merged, indent=2))


def merge_local_tree(kind: str, source_dir: str, merged_output_path: str) -> None:
    root = Path(source_dir)
    if not root.exists():
        return
    for payload_path in sorted(root.rglob("*.json")):
        if payload_path.name.endswith("_merged.json"):
            continue
        if kind == "wall_clock" and payload_path.name != "allqueries_wall_clock.json":
            continue
        if kind == "stage_metrics" and payload_path.name != "stage_metrics.json":
            continue
        payload = json.loads(payload_path.read_text())
        merge_payload(kind, payload, merged_output_path)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge benchmark payloads idempotently.")
    parser.add_argument("--kind", choices=["wall_clock", "stage_metrics"], required=True)
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--merged-output", required=True)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    merge_local_tree(args.kind, args.source_dir, args.merged_output)


if __name__ == "__main__":
    main()
