"""
Capture qualitative execution evidence for selected queries.
Saves df.explain(extended=True) and rdd.toDebugString() to results/plans/

Usage (local):
    python -m benchmark.plans \
        --parquet-path data/processed/access_logs \
        --output-dir results/plans

Usage (Dataproc):
    gcloud dataproc jobs submit pyspark gs://.../plans.py \
        --py-files gs://.../src.zip
        [-- --parquet-path gs://... --output-dir gs://...]
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

import io
from contextlib import redirect_stdout


from src.session import get_spark_session, load_env

from src.queries.error_pattern_analysis.RDD.pipeline import build_queries as rdd_build
from src.queries.error_pattern_analysis.DataFrame.pipeline import build_queries as df_build

from src.queries.perhost_traffic_profiling.RDD.pipeline import build_queries as rdd_build_traffic
from src.queries.perhost_traffic_profiling.DataFrame.pipeline import build_queries as df_build_traffic

from src.queries.temporal_aggregation.RDD.pipeline import build_queries as rdd_build_temporal
from src.queries.temporal_aggregation.DataFrame.pipeline import build_queries as df_build_temporal

from src.queries.sessionization.RDD.pipeline import build_queries as rdd_build_sessionization
from src.queries.sessionization.DataFrame.pipeline import build_queries as df_build_sessionization

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
    print(f"[plans] saved {output_path}", flush=True)

def _capture_df_plan(df, query: str, output_dir: str) -> None:
    buf = io.StringIO()
    with redirect_stdout(buf):
        df.explain(extended=True)
    _write_results(buf.getvalue(), f"{output_dir}/{query}_df_plan.txt")

def _capture_rdd_lineage(rdd, query: str, output_dir: str) -> None:
    _write_results(rdd.toDebugString().decode("utf-8"), f"{output_dir}/{query}_rdd_lineage.txt")

def _run_and_capture_plans_traffic_profiling(
    spark: SparkSession,
    parquet_path: str,
    label: str,
    output_dir: str,
):
    """
    Run the specified query and capture its execution plan.
    """
    print(f"[plans] starting perhost_traffic_profiling/{label} ...", flush=True)
    if label == "DataFrame":
        traffic_profiling_df = df_build_traffic(spark, parquet_path)
        _capture_df_plan(traffic_profiling_df, "traffic_profiling", output_dir)
    elif label == "RDD":
        traffic_profiling_rdd = rdd_build_traffic(spark, parquet_path)
        _capture_rdd_lineage(traffic_profiling_rdd, "traffic_profiling", output_dir)
    print(f"[plans] finished perhost_traffic_profiling/{label}", flush=True)

    

def _run_and_capture_plans_temporal_aggregation(
    spark: SparkSession,
    parquet_path: str,
    label: str,
    output_dir: str,
):
    """ I choice metrics_per_hour DF/RDD because it has more transformations and shuffles"""
    print(f"[plans] starting temporal_aggregation/{label} ...", flush=True)
    if label == "DataFrame":
        metrics_per_hour_df, _ = df_build_temporal(spark, parquet_path)
        _capture_df_plan(metrics_per_hour_df, "temporal_agg", output_dir)
    elif label == "RDD":
        metrics_per_hour_rdd, _ = rdd_build_temporal(spark, parquet_path)
        _capture_rdd_lineage(metrics_per_hour_rdd, "temporal_agg", output_dir)
    print(f"[plans] finished temporal_aggregation/{label}", flush=True)


def _run_and_capture_plans_error_pattern_analysis(
    spark: SparkSession,
    parquet_path: str,
    label: str,
    output_dir: str,
): 
    """ I choose top_endpoints DF/RDD because it has more transformations and shuffles"""
    print(f"[plans] starting error_pattern_analysis/{label} ...", flush=True)
    if label == "DataFrame":
        top_endpoints_df, _ = df_build(spark, parquet_path)
        _capture_df_plan(top_endpoints_df, "error_pattern", output_dir)
    elif label == "RDD":
        top_endpoints_rdd, _ = rdd_build(spark, parquet_path)
        _capture_rdd_lineage(top_endpoints_rdd, "error_pattern", output_dir)
    print(f"[plans] finished error_pattern_analysis/{label}", flush=True)
    
def _run_and_capture_plans_sessionization(
    spark: SparkSession,
    parquet_path: str,
    label: str,
    output_dir: str,
): 
    """ I choice sessions DF/RDD because it has more transformations and shuffles. Also includes window function."""
    if label == "DataFrame":
        sessions_df, _ = df_build_sessionization(spark, parquet_path)
        _capture_df_plan(sessions_df, "sessionization", output_dir)
    elif label == "RDD":
        sessions_rdd, _ = rdd_build_sessionization(spark, parquet_path)
        _capture_rdd_lineage(sessions_rdd, "sessionization", output_dir)
    print(f"[plans] finished sessionization/{label}", flush=True)

def _parse_args() -> argparse.Namespace:
    load_env()
    load_env(".env.dataproc")
    parser = argparse.ArgumentParser(description="Capture execution plans for selected queries.")
    parser.add_argument("--parquet-path", default=os.environ.get("OUTPUT_PARQUET_PATH", ""))
    parser.add_argument("--output-dir", default=os.environ.get("RESULTS_PATH", "results/plans"))
    parser.add_argument("--scale-pct", type=int, default=None)
    parser.add_argument("--benchmark-id", default=None)
    return parser.parse_args()

def main() -> None:
    args = _parse_args()

    if not args.parquet_path:
        raise ValueError(
            "--parquet-path is required (or set OUTPUT_PARQUET_PATH in .env.dataproc)"
        )
    
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    benchmark_id = args.benchmark_id or datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    output_dir = args.output_dir
    parquet_path = args.parquet_path

    _write_results(
        json.dumps(
            {
                "benchmark_id": benchmark_id,
                "scale_pct": args.scale_pct,
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
        ),
        f"{output_dir}/run_manifest.json",
    )

    for label in ["DataFrame", "RDD"]:
        _run_and_capture_plans_error_pattern_analysis(spark, parquet_path, label, output_dir)
        _run_and_capture_plans_temporal_aggregation(spark, parquet_path, label, output_dir)
        _run_and_capture_plans_traffic_profiling(spark, parquet_path, label, output_dir)
        _run_and_capture_plans_sessionization(spark, parquet_path, label, output_dir)
        
if __name__ == "__main__":
    main()