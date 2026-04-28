"""
Combined Dataproc entry point: wall-clock timing and stage metrics in one Spark job.

Runs the same workloads as benchmark.wall_clock and benchmark.stage_metrics sequentially
in a single SparkSession, writing two JSON outputs (same schema as each standalone script).

Usage (Dataproc):
    gcloud dataproc jobs submit pyspark gs://.../run_benchmark.py \\
        --py-files gs://.../src.zip \\
        [-- --parquet-path gs://... --wall-clock-output-path ...]
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from typing import Any

from benchmark.merge_results import merge_payload
from benchmark import wall_clock as wc
from benchmark import stage_metrics as sm
from src.session import get_spark_session, load_env


def _parse_args() -> argparse.Namespace:
    load_env()
    load_env(".env.dataproc")
    parser = argparse.ArgumentParser(
        description="Combined wall-clock + stage-metrics benchmark (single Spark job)",
    )
    parser.add_argument(
        "--parquet-path",
        default=os.environ.get("OUTPUT_PARQUET_PATH", ""),
        help="Path to preprocessed Parquet data (local or gs://)",
    )
    parser.add_argument("--scale-pct", type=int, default=None)
    parser.add_argument("--benchmark-id", default=None)
    parser.add_argument(
        "--wall-clock-output-path",
        default=os.environ.get(
            "RESULTS_PATH", "results/allqueries_wall_clock.json"
        ),
        help="Wall-clock JSON destination",
    )
    parser.add_argument(
        "--wall-clock-merged-output-path",
        default=None,
        help="Optional merged rollup for wall-clock results",
    )
    parser.add_argument(
        "--stage-metrics-output-path",
        default=os.environ.get(
            "STAGE_METRICS_RESULTS_PATH", "results/stage_metrics.json"
        ),
        help="Stage-metrics JSON destination",
    )
    parser.add_argument(
        "--stage-metrics-merged-output-path",
        default=None,
        help="Optional merged rollup for stage-metrics results",
    )
    parser.add_argument(
        "--wall-clock-num-runs",
        type=int,
        default=int(os.environ.get("WALL_CLOCK_NUM_RUNS", "1")),
        help="Repeats per query/API for wall-clock (env: WALL_CLOCK_NUM_RUNS)",
    )
    parser.add_argument(
        "--stage-metrics-num-runs",
        type=int,
        default=int(os.environ.get("STAGE_METRICS_NUM_RUNS", "1")),
        help="Repeats per query/API for stage metrics (env: STAGE_METRICS_NUM_RUNS)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if not args.parquet_path:
        raise ValueError(
            "--parquet-path is required (or set OUTPUT_PARQUET_PATH in .env.dataproc)"
        )
    if args.wall_clock_num_runs < 1:
        raise ValueError("--wall-clock-num-runs must be >= 1")
    if args.stage_metrics_num_runs < 1:
        raise ValueError("--stage-metrics-num-runs must be >= 1")

    benchmark_id = args.benchmark_id or datetime.now(timezone.utc).strftime(
        "%Y%m%d-%H%M%S"
    )

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    apis = ["RDD", "DataFrame", "SQL"]

    # --- Wall-clock section ---
    wc_error_pattern_runs = wc._run_query_many(
        "error_pattern_analysis",
        wc._time_run_error_pattern,
        spark,
        args.parquet_path,
        apis,
        args.wall_clock_num_runs,
    )
    wc_temporal_runs = wc._run_query_many(
        "temporal_aggregation",
        wc._time_run_temporal_aggregation,
        spark,
        args.parquet_path,
        apis,
        args.wall_clock_num_runs,
    )
    wc_traffic_runs = wc._run_query_many(
        "perhost_traffic_profiling",
        wc._time_run_traffic_profiling,
        spark,
        args.parquet_path,
        apis,
        args.wall_clock_num_runs,
    )
    wc_traffic_naive_runs = wc._run_query_many(
        "perhost_traffic_profiling_naive",
        wc._time_run_traffic_profiling_naive,
        spark,
        args.parquet_path,
        apis,
        args.wall_clock_num_runs,
    )
    wc_sessionization_runs = wc._run_query_many(
        "sessionization",
        wc._time_run_sessionization,
        spark,
        args.parquet_path,
        apis,
        args.wall_clock_num_runs,
    )

    wall_clock_results: dict[str, Any] = {
        "benchmark_id": benchmark_id,
        "scale_pct": args.scale_pct,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "num_runs": args.wall_clock_num_runs,
        "error_pattern_analysis": wc_error_pattern_runs,
        "temporal_aggregation": wc_temporal_runs,
        "perhost_traffic_profiling": wc_traffic_runs,
        "perhost_traffic_profiling_naive": wc_traffic_naive_runs,
        "sessionization": wc_sessionization_runs,
        "summary": {
            "error_pattern_analysis": wc._summarize_elapsed(wc_error_pattern_runs),
            "temporal_aggregation": wc._summarize_elapsed(wc_temporal_runs),
            "perhost_traffic_profiling": wc._summarize_elapsed(wc_traffic_runs),
            "perhost_traffic_profiling_naive": wc._summarize_elapsed(
                wc_traffic_naive_runs
            ),
            "sessionization": wc._summarize_elapsed(wc_sessionization_runs),
        },
    }

    wc_payload = json.dumps(wall_clock_results, indent=2)
    print(wc_payload, flush=True)
    wc._write_results(wc_payload, args.wall_clock_output_path)
    if args.wall_clock_merged_output_path:
        merge_payload(
            "wall_clock", wall_clock_results, args.wall_clock_merged_output_path
        )
        print(
            f"[benchmark] wall-clock merged upserted to {args.wall_clock_merged_output_path}",
            flush=True,
        )

    # --- Stage metrics section (same Spark application; REST API sees cumulative stages) ---
    ui_url = spark.sparkContext.uiWebUrl
    if ui_url is None:
        raise RuntimeError(
            "Spark UI is disabled (spark.ui.enabled=false); stage metrics require it."
        )
    app_id = spark.sparkContext.applicationId
    print(f"[stages] Spark UI at {ui_url}, app_id={app_id}", flush=True)

    sm_error_pattern_runs = sm._run_query_many(
        "error_pattern_analysis",
        sm._stage_run_error_pattern,
        spark,
        args.parquet_path,
        ui_url,
        app_id,
        apis,
        args.stage_metrics_num_runs,
    )
    sm_temporal_runs = sm._run_query_many(
        "temporal_aggregation",
        sm._stage_run_temporal_aggregation,
        spark,
        args.parquet_path,
        ui_url,
        app_id,
        apis,
        args.stage_metrics_num_runs,
    )
    sm_traffic_runs = sm._run_query_many(
        "perhost_traffic_profiling",
        sm._stage_run_traffic_profiling,
        spark,
        args.parquet_path,
        ui_url,
        app_id,
        apis,
        args.stage_metrics_num_runs,
    )
    sm_traffic_naive_runs = sm._run_query_many(
        "perhost_traffic_profiling_naive",
        sm._stage_run_traffic_profiling_naive,
        spark,
        args.parquet_path,
        ui_url,
        app_id,
        apis,
        args.stage_metrics_num_runs,
    )
    sm_sessionization_runs = sm._run_query_many(
        "sessionization",
        sm._stage_run_sessionization,
        spark,
        args.parquet_path,
        ui_url,
        app_id,
        apis,
        args.stage_metrics_num_runs,
    )

    stage_metrics_results: dict[str, Any] = {
        "benchmark_id": benchmark_id,
        "scale_pct": args.scale_pct,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "num_runs": args.stage_metrics_num_runs,
        "error_pattern_analysis": sm_error_pattern_runs,
        "temporal_aggregation": sm_temporal_runs,
        "perhost_traffic_profiling": sm_traffic_runs,
        "perhost_traffic_profiling_naive": sm_traffic_naive_runs,
        "sessionization": sm_sessionization_runs,
        "summary": {
            "error_pattern_analysis": sm._summarize_stage_metrics(
                sm_error_pattern_runs
            ),
            "temporal_aggregation": sm._summarize_stage_metrics(sm_temporal_runs),
            "perhost_traffic_profiling": sm._summarize_stage_metrics(sm_traffic_runs),
            "perhost_traffic_profiling_naive": sm._summarize_stage_metrics(
                sm_traffic_naive_runs
            ),
            "sessionization": sm._summarize_stage_metrics(sm_sessionization_runs),
        },
    }

    sm_payload = json.dumps(stage_metrics_results, indent=2)
    print(sm_payload, flush=True)
    sm._write_results(sm_payload, args.stage_metrics_output_path)
    if args.stage_metrics_merged_output_path:
        merge_payload(
            "stage_metrics",
            stage_metrics_results,
            args.stage_metrics_merged_output_path,
        )
        print(
            f"[benchmark] stage-metrics merged upserted to {args.stage_metrics_merged_output_path}",
            flush=True,
        )

    print(
        "[benchmark] combined run complete (wall-clock + stage-metrics)",
        flush=True,
    )


if __name__ == "__main__":
    main()
