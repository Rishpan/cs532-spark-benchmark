"""
Stage metrics benchmark for all queries across all three Spark APIs.

Usage (local):
    python -m benchmark.stage_metrics \
        --parquet-path data/processed/access_logs \
        --output-path results/stage_metrics.json

Usage (Dataproc):
    gcloud dataproc jobs submit pyspark gs://.../stages.py \
        --py-files gs://.../src.zip
        [-- --parquet-path gs://... --output-path gs://...]
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import requests
from pyspark.sql import SparkSession

from src.session import get_spark_session, load_env

from src.queries.error_pattern_analysis.RDD.pipeline import build_queries as rdd_build
from src.queries.error_pattern_analysis.DataFrame.pipeline import build_queries as df_build
from src.queries.error_pattern_analysis.SQL.pipeline import build_queries as sql_build

from src.queries.perhost_traffic_profiling.RDD.pipeline import build_queries as rdd_build_traffic
from src.queries.perhost_traffic_profiling.SQL.pipeline import build_queries as sql_build_traffic
from src.queries.perhost_traffic_profiling.DataFrame.pipeline import build_queries as df_build_traffic

from src.queries.perhost_traffic_profiling.RDD.naive_pipeline import build_queries as rdd_build_traffic_naive
from src.queries.perhost_traffic_profiling.SQL.naive_pipeline import build_queries as sql_build_traffic_naive
from src.queries.perhost_traffic_profiling.DataFrame.naive_pipeline import build_queries as df_build_traffic_naive

from src.queries.temporal_aggregation.RDD.pipeline import build_queries as rdd_build_temporal
from src.queries.temporal_aggregation.SQL.pipeline import build_queries as sql_build_temporal
from src.queries.temporal_aggregation.DataFrame.pipeline import build_queries as df_build_temporal

_VIEW_NAME = "zanbil_logs_view"


def _get_stages(ui_url: str, app_id: str) -> list[dict]:
    """Fetch all completed stages from the Spark REST API."""
    url = f"{ui_url}/api/v1/applications/{app_id}/stages"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print("Unable to fetch stages from Spark REST API:", e)
        return []


def _snapshot_stage_ids(ui_url: str, app_id: str) -> set[int]:
    """Return the set of stage IDs already known before a run."""
    stage_id_set = set()
    stages = _get_stages(ui_url, app_id)
    for s in stages:
        stage_id_set.add(s["stageId"])
    return stage_id_set


def _collect_stage_metrics(
    ui_url: str,
    app_id: str,
    pre_run_ids: set[int],
) -> dict[str, Any]:
    """
    Get aggregate metrics for all stages that ran since the pre-run snapshot. Metrics include:
    - num_stages: number of stages that ran
    - num_tasks: total number of tasks across those stages
    - shuffle_read_bytes: total shuffle read bytes across those stages
    - shuffle_write_bytes: total shuffle write bytes across those stages
    - disk_bytes_spilled: total disk bytes spilled across those stages
    - executor_deserialize_time_sec: total executor deserialize time across those stages (in seconds)
    - executor_cpu_time_sec: total executor CPU time across those stages (in seconds)
    """
    all_stages = _get_stages(ui_url, app_id)
    new_stages = [s for s in all_stages if s["stageId"] not in pre_run_ids]
    return {
        "num_stages": len(new_stages),
        "num_tasks": sum(s.get("numTasks", 0) for s in new_stages),
        "shuffle_read_bytes": sum(s.get("shuffleReadBytes", 0) for s in new_stages),
        "shuffle_write_bytes": sum(s.get("shuffleWriteBytes", 0) for s in new_stages),
        "disk_bytes_spilled": sum(s.get("diskBytesSpilled", 0) for s in new_stages),
        "executor_deserialize_time_sec": sum(s.get("executorDeserializeTime", 0) for s in new_stages) / 1000,
        "executor_cpu_time_sec": sum(s.get("executorCpuTime", 0) for s in new_stages) / 1e9
    }



def _stage_run_error_pattern(
    label: str, spark: SparkSession, parquet_path: str,
    ui_url: str, app_id: str,
) -> dict[str, Any]:
    print(f"[stages] starting error_pattern/{label} ...", flush=True)
    pre = _snapshot_stage_ids(ui_url, app_id)

    if label == "RDD":
        top_ep, err_freq = rdd_build(spark, parquet_path)
        top_ep.count(); err_freq.count()
    elif label == "DataFrame":
        top_ep, err_freq = df_build(spark, parquet_path)
        top_ep.count(); err_freq.count()
    elif label == "SQL":
        top_ep, err_freq = sql_build(spark, parquet_path, _VIEW_NAME)
        top_ep.count(); err_freq.count()

    # Must pass in pre to see which stages are new since the pre-run snapshot
    metrics = _collect_stage_metrics(ui_url, app_id, pre)
    print(f"[stages] error_pattern_analysis/{label}: {metrics}", flush=True)
    return {"api": label, **metrics}


def _stage_run_temporal_aggregation(
    label: str, spark: SparkSession, parquet_path: str,
    ui_url: str, app_id: str,
) -> dict[str, Any]:
    print(f"[stages] starting temporal_aggregation/{label} ...", flush=True)
    pre = _snapshot_stage_ids(ui_url, app_id)

    if label == "RDD":
        metrics_per_hour, metrics_per_day = rdd_build_temporal(spark, parquet_path)
        metrics_per_hour.count(); metrics_per_day.count()
    elif label == "DataFrame":
        metrics_per_hour, metrics_per_day = df_build_temporal(spark, parquet_path)
        metrics_per_hour.count(); metrics_per_day.count()
    elif label == "SQL":
        metrics_per_hour, metrics_per_day = sql_build_temporal(spark, parquet_path, _VIEW_NAME)
        metrics_per_hour.count(); metrics_per_day.count()

    # Must pass in pre to see which stages are new since the pre-run snapshot
    metrics = _collect_stage_metrics(ui_url, app_id, pre)
    print(f"[stages] temporal_aggregation/{label}: {metrics}", flush=True)
    return {"api": label, **metrics}


def _stage_run_traffic_profiling(
    label: str, spark: SparkSession, parquet_path: str,
    ui_url: str, app_id: str,
) -> dict[str, Any]:
    print(f"[stages] starting traffic_profiling/{label} ...", flush=True)
    pre = _snapshot_stage_ids(ui_url, app_id)

    if label == "RDD":
        rdd_build_traffic(spark, parquet_path).count()
    elif label == "DataFrame":
        df_build_traffic(spark, parquet_path).count()
    elif label == "SQL":
        sql_build_traffic(spark, parquet_path, _VIEW_NAME).count()

    # Must pass in pre to see which stages are new since the pre-run snapshot
    metrics = _collect_stage_metrics(ui_url, app_id, pre)
    print(f"[stages] perhost_traffic_profiling/{label}: {metrics}", flush=True)
    return {"api": label, **metrics}


def _stage_run_traffic_profiling_naive(
    label: str, spark: SparkSession, parquet_path: str,
    ui_url: str, app_id: str,
) -> dict[str, Any]:
    print(f"[stages] starting traffic_profiling_naive/{label} ...", flush=True)
    pre = _snapshot_stage_ids(ui_url, app_id)

    if label == "RDD":
        out = rdd_build_traffic_naive(spark, parquet_path)
        for res in out: out[res].count()
    elif label == "DataFrame":
        out = df_build_traffic_naive(spark, parquet_path)
        for res in out: out[res].count()
    elif label == "SQL":
        out = sql_build_traffic_naive(spark, parquet_path, _VIEW_NAME)
        for res in out: out[res].count()

    # Must pass in pre to see which stages are new since the pre-run snapshot
    metrics = _collect_stage_metrics(ui_url, app_id, pre)
    print(f"[stages] perhost_traffic_profiling_naive/{label}: {metrics}", flush=True)
    return {"api": label, **metrics}

def _parse_args() -> argparse.Namespace:
    load_env()
    load_env(".env.dataproc")
    parser = argparse.ArgumentParser(description="Stage/task benchmark")
    parser.add_argument("--parquet-path", default=os.environ.get("OUTPUT_PARQUET_PATH", ""))
    parser.add_argument("--output-path", default=os.environ.get("RESULTS_PATH", "results/stage_metrics.json"))
    parser.add_argument(
        "--num-runs",
        type=int,
        default=int(os.environ.get("STAGE_METRICS_NUM_RUNS", "1")),
        help="Number of times to run each query/API variant (default: STAGE_METRICS_NUM_RUNS or 1)",
    )
    return parser.parse_args()


def _run_query_many(
    query_name: str,
    run_fn,
    spark: SparkSession,
    parquet_path: str,
    ui_url: str,
    app_id: str,
    apis: list[str],
    num_runs: int,
) -> list[dict[str, Any]]:
    """Run one query family across APIs for num_runs and return per-run records."""
    records: list[dict[str, Any]] = []
    for run in range(1, num_runs + 1):
        for api in apis:
            print(
                f"[stages] run {run}/{num_runs} starting {query_name}/{api} ...",
                flush=True,
            )
            entry = run_fn(api, spark, parquet_path, ui_url, app_id)
            entry["run"] = run
            records.append(entry)
    return records


def _summarize_stage_metrics(
    records: list[dict[str, Any]],
) -> dict[str, dict[str, float | int]]:
    """Aggregate avg/std for all numeric stage metrics by API."""
    metric_names = [
        "num_stages",
        "num_tasks",
        "shuffle_read_bytes",
        "shuffle_write_bytes",
        "disk_bytes_spilled",
        "executor_deserialize_time_sec",
        "executor_cpu_time_sec",
    ]
    by_api: dict[str, list[dict[str, Any]]] = {}
    for rec in records:
        by_api.setdefault(str(rec["api"]), []).append(rec)

    summary: dict[str, dict[str, float | int]] = {}
    for api, api_records in by_api.items():
        api_summary: dict[str, float | int] = {"num_runs": len(api_records)}
        for metric in metric_names:
            vals = [float(rec[metric]) for rec in api_records]
            avg = statistics.fmean(vals)
            std = statistics.pstdev(vals) if len(vals) > 1 else 0.0
            api_summary[f"{metric}_avg"] = round(avg, 3)
            api_summary[f"{metric}_std"] = round(std, 3)
        summary[api] = api_summary
    return summary


def _write_results(content: str, output_path: str) -> None:
    if output_path.startswith("gs://"):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(content)
            tmp_path = f.name
        try:
            subprocess.run(["gcloud", "storage", "cp", tmp_path, output_path], check=True)
        finally:
            Path(tmp_path).unlink(missing_ok=True)
    else:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(content)
    print(f"[stages] results written to {output_path}", flush=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = _parse_args()
    if not args.parquet_path:
        raise ValueError("--parquet-path is required")
    if args.num_runs < 1:
        raise ValueError("--num-runs must be >= 1")

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    ui_url = spark.sparkContext.uiWebUrl
    if ui_url is None:
        raise RuntimeError("Spark UI is disabled (spark.ui.enabled=false); stage metrics require it.")
    app_id = spark.sparkContext.applicationId
    print(f"[stages] Spark UI at {ui_url}, app_id={app_id}", flush=True)

    apis = ["RDD", "DataFrame", "SQL"]

    error_pattern_runs = _run_query_many(
        "error_pattern_analysis",
        _stage_run_error_pattern,
        spark,
        args.parquet_path,
        ui_url,
        app_id,
        apis,
        args.num_runs,
    )
    temporal_aggregation_runs = _run_query_many(
        "temporal_aggregation",
        _stage_run_temporal_aggregation,
        spark,
        args.parquet_path,
        ui_url,
        app_id,
        apis,
        args.num_runs,
    )
    traffic_profiling_runs = _run_query_many(
        "perhost_traffic_profiling",
        _stage_run_traffic_profiling,
        spark,
        args.parquet_path,
        ui_url,
        app_id,
        apis,
        args.num_runs,
    )
    traffic_profiling_naive_runs = _run_query_many(
        "perhost_traffic_profiling_naive",
        _stage_run_traffic_profiling_naive,
        spark,
        args.parquet_path,
        ui_url,
        app_id,
        apis,
        args.num_runs,
    )

    results = {
        "error_pattern_analysis": error_pattern_runs,
        "temporal_aggregation": temporal_aggregation_runs,
        "perhost_traffic_profiling": traffic_profiling_runs,
        "perhost_traffic_profiling_naive": traffic_profiling_naive_runs,
        "summary": {
            "error_pattern_analysis": _summarize_stage_metrics(error_pattern_runs),
            "temporal_aggregation": _summarize_stage_metrics(temporal_aggregation_runs),
            "perhost_traffic_profiling": _summarize_stage_metrics(traffic_profiling_runs),
            "perhost_traffic_profiling_naive": _summarize_stage_metrics(traffic_profiling_naive_runs),
        },
    }

    payload = json.dumps(results, indent=2)
    print(payload, flush=True)
    _write_results(payload, args.output_path)


if __name__ == "__main__":
    main()