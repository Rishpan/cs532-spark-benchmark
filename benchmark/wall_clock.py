"""
Wall-clock benchmark for error_pattern_analysis across all three Spark APIs.

Usage (local):
    python -m benchmark.wall_clock \
        --parquet-path data/processed/access_logs \
        --output-path results/allqueries_wall_clock.json

Usage (Dataproc — paths default to .env.dataproc values):
    gcloud dataproc jobs submit pyspark gs://.../wall_clock.py \
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
import time
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from src.session import get_spark_session, load_env

from src.queries.error_pattern_analysis.RDD.pipeline import (
    build_queries as rdd_build,
)
from src.queries.error_pattern_analysis.DataFrame.pipeline import (
    build_queries as df_build,
)
from src.queries.error_pattern_analysis.SQL.pipeline import (
    build_queries as sql_build,
)

from src.queries.perhost_traffic_profiling.RDD.pipeline import (
    build_queries as rdd_build_traffic,
)

from src.queries.perhost_traffic_profiling.SQL.pipeline import (
    build_queries as sql_build_traffic,
)

from src.queries.perhost_traffic_profiling.DataFrame.pipeline import (
    build_queries as df_build_traffic,
)

from src.queries.perhost_traffic_profiling.RDD.naive_pipeline import (
    build_queries as rdd_build_traffic_naive,
)

from src.queries.perhost_traffic_profiling.SQL.naive_pipeline import (
    build_queries as sql_build_traffic_naive,
)

from src.queries.perhost_traffic_profiling.DataFrame.naive_pipeline import (
    build_queries as df_build_traffic_naive,
)

from src.queries.temporal_aggregation.RDD.pipeline import (
    build_queries as rdd_build_temporal,
)

from src.queries.temporal_aggregation.SQL.pipeline import (
    build_queries as sql_build_temporal,
)

from src.queries.temporal_aggregation.DataFrame.pipeline import (
    build_queries as df_build_temporal,
)

_VIEW_NAME = "zanbil_logs_view"


def _parse_args() -> argparse.Namespace:
    load_env()             # loads .env (local master, JAVA_TOOL_OPTIONS, etc.)
    load_env(".env.dataproc")  # overlays Dataproc paths if present
    parser = argparse.ArgumentParser(
        description="Wall-clock benchmark for error_pattern_analysis"
    )
    parser.add_argument(
        "--parquet-path",
        default=os.environ.get("OUTPUT_PARQUET_PATH", ""),
        help="Path to preprocessed Parquet data (local or gs://)",
    )
    parser.add_argument(
        "--output-path",
        default=os.environ.get("RESULTS_PATH", "results/allqueries_wall_clock.json"),
        help="Destination for the JSON results file (local or gs://)",
    )
    parser.add_argument(
        "--num-runs",
        type=int,
        default=int(os.environ.get("WALL_CLOCK_NUM_RUNS", "1")),
        help="Number of times to run each query/API variant (default: WALL_CLOCK_NUM_RUNS or 1)",
    )
    return parser.parse_args()


def _time_run_error_pattern(label: str, spark: SparkSession, parquet_path: str) -> dict[str, Any]:
    """Run one API variant and return its wall-clock time in seconds."""
    print(f"[benchmark] starting error_pattern_analysis/{label} ...", flush=True)
    start = time.perf_counter()

    if label == "RDD":
        top_ep, err_freq = rdd_build(spark, parquet_path)
        top_ep.count()
        err_freq.count()
    elif label == "DataFrame":
        top_ep, err_freq = df_build(spark, parquet_path)
        top_ep.count()
        err_freq.count()
    elif label == "SQL":
        top_ep, err_freq = sql_build(spark, parquet_path, _VIEW_NAME)
        top_ep.count()
        err_freq.count()

    elapsed = time.perf_counter() - start
    print(f"[benchmark] error_pattern_analysis/{label} finished in {elapsed:.3f}s", flush=True)
    return {"api": label, "elapsed_sec": round(elapsed, 3)}

def _time_run_temporal_aggregation(label: str, spark: SparkSession, parquet_path: str) -> dict[str, Any]:
    """Run one API variant and return its wall-clock time in seconds."""
    print(f"[benchmark] starting temporal_aggregation/{label} ...", flush=True)
    start = time.perf_counter()

    if label == "RDD":
        metrics_per_hour, metrics_per_day = rdd_build_temporal(spark, parquet_path)
        metrics_per_hour.count()
        metrics_per_day.count()
    elif label == "DataFrame":
        metrics_per_hour, metrics_per_day = df_build_temporal(spark, parquet_path)
        metrics_per_hour.count()
        metrics_per_day.count()
    elif label == "SQL":
        metrics_per_hour, metrics_per_day = sql_build_temporal(spark, parquet_path, _VIEW_NAME)
        metrics_per_hour.count()
        metrics_per_day.count()

    elapsed = time.perf_counter() - start
    print(f"[benchmark] temporal_aggregation/{label} finished in {elapsed:.3f}s", flush=True)
    return {"api": label, "elapsed_sec": round(elapsed, 3)}

def _time_run_traffic_profiling(label: str, spark: SparkSession, parquet_path: str) -> dict[str, Any]:
    """Run one API variant and return its wall-clock time in seconds."""
    print(f"[benchmark] starting perhost_traffic_profiling/{label} ...", flush=True)
    start = time.perf_counter()

    if label == "RDD":
        metrics_per_host = rdd_build_traffic(spark, parquet_path)
        metrics_per_host.count()
    elif label == "DataFrame":
        metrics_per_host = df_build_traffic(spark, parquet_path)
        metrics_per_host.count()
    elif label == "SQL":
        metrics_per_host = sql_build_traffic(spark, parquet_path, _VIEW_NAME)
        metrics_per_host.count()

    elapsed = time.perf_counter() - start
    print(f"[benchmark] perhost_traffic_profiling/{label} finished in {elapsed:.3f}s", flush=True)
    return {"api": label, "elapsed_sec": round(elapsed, 3)}

def _time_run_traffic_profiling_naive(label: str, spark: SparkSession, parquet_path: str) -> dict[str, Any]:
    """Run one API variant and return its wall-clock time in seconds."""
    print(f"[benchmark] starting perhost_traffic_profiling_naive/{label} ...", flush=True)
    start = time.perf_counter()

    if label == "RDD":
        out = rdd_build_traffic_naive(spark, parquet_path)
        for res in out:
            out[res].count()
    elif label == "DataFrame":
        out = df_build_traffic_naive(spark, parquet_path)
        for res in out:
            out[res].count()
    elif label == "SQL":
        out = sql_build_traffic_naive(spark, parquet_path, _VIEW_NAME)
        for res in out:
            out[res].count()

    elapsed = time.perf_counter() - start
    print(f"[benchmark] perhost_traffic_profiling_naive/{label} finished in {elapsed:.3f}s", flush=True)
    return {"api": label, "elapsed_sec": round(elapsed, 3)}


def _run_query_many(
    query_name: str,
    run_fn,
    spark: SparkSession,
    parquet_path: str,
    apis: list[str],
    num_runs: int,
) -> list[dict[str, Any]]:
    """Run one query family across APIs for num_runs and return per-run records."""
    records: list[dict[str, Any]] = []
    for run in range(1, num_runs + 1):
        for api in apis:
            print(
                f"[benchmark] run {run}/{num_runs} starting {query_name}/{api} ...",
                flush=True,
            )
            entry = run_fn(api, spark, parquet_path)
            entry["run"] = run
            records.append(entry)
    return records


def _summarize_elapsed(records: list[dict[str, Any]]) -> dict[str, dict[str, float | int]]:
    """Aggregate avg/std elapsed by API from per-run records."""
    by_api: dict[str, list[float]] = {}
    for rec in records:
        by_api.setdefault(str(rec["api"]), []).append(float(rec["elapsed_sec"]))

    summary: dict[str, dict[str, float | int]] = {}
    for api, vals in by_api.items():
        avg = statistics.fmean(vals)
        std = statistics.pstdev(vals) if len(vals) > 1 else 0.0
        summary[api] = {
            "num_runs": len(vals),
            "avg_elapsed_sec": round(avg, 3),
            "std_elapsed_sec": round(std, 3),
        }
    return summary


def _write_results(content: str, output_path: str) -> None:
    """Write content to output_path.

    Uses gsutil cp for gs:// paths (available on all Dataproc nodes and via
    the gcloud SDK locally). Uses Path.write_text for local paths.
    """
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
    print(f"[benchmark] results written to {output_path}", flush=True)


def main() -> None:
    args = _parse_args()

    if not args.parquet_path:
        raise ValueError(
            "--parquet-path is required (or set OUTPUT_PARQUET_PATH in .env.dataproc)"
        )
    if args.num_runs < 1:
        raise ValueError("--num-runs must be >= 1")

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    apis = ["RDD", "DataFrame", "SQL"]

    error_pattern_runs = _run_query_many(
        "error_pattern_analysis",
        _time_run_error_pattern,
        spark,
        args.parquet_path,
        apis,
        args.num_runs,
    )
    temporal_aggregation_runs = _run_query_many(
        "temporal_aggregation",
        _time_run_temporal_aggregation,
        spark,
        args.parquet_path,
        apis,
        args.num_runs,
    )
    traffic_profiling_runs = _run_query_many(
        "perhost_traffic_profiling",
        _time_run_traffic_profiling,
        spark,
        args.parquet_path,
        apis,
        args.num_runs,
    )
    traffic_profiling_naive_runs = _run_query_many(
        "perhost_traffic_profiling_naive",
        _time_run_traffic_profiling_naive,
        spark,
        args.parquet_path,
        apis,
        args.num_runs,
    )

    # TODO: Add timing for sessionization query

    results = {
        "error_pattern_analysis": error_pattern_runs,
        "temporal_aggregation": temporal_aggregation_runs,
        "perhost_traffic_profiling": traffic_profiling_runs,
        "perhost_traffic_profiling_naive": traffic_profiling_naive_runs,
        "summary": {
            "error_pattern_analysis": _summarize_elapsed(error_pattern_runs),
            "temporal_aggregation": _summarize_elapsed(temporal_aggregation_runs),
            "perhost_traffic_profiling": _summarize_elapsed(traffic_profiling_runs),
            "perhost_traffic_profiling_naive": _summarize_elapsed(traffic_profiling_naive_runs),
        },
    }
    payload = json.dumps(results, indent=2)
    print(payload, flush=True)

    _write_results(payload, args.output_path)


if __name__ == "__main__":
    main()
