"""
Wall-clock benchmark for error_pattern_analysis across all three Spark APIs.

Usage (local):
    python -m benchmark.wall_clock \
        --parquet-path data/processed/access_logs \
        --output-path results/error_pattern_wall_clock.json

Usage (Dataproc — paths default to .env.dataproc values):
    gcloud dataproc jobs submit pyspark gs://.../wall_clock.py \
        --py-files gs://.../src.zip
        [-- --parquet-path gs://... --output-path gs://...]
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from src.session import load_env

from src.queries.error_pattern_analysis.RDD.pipeline import (
    build_queries as rdd_build,
)
from src.queries.error_pattern_analysis.DataFrame.pipeline import (
    build_queries as df_build,
)
from src.queries.error_pattern_analysis.SQL.pipeline import (
    build_queries as sql_build,
)

_VIEW_NAME = "zanbil_logs_view"


def _parse_args() -> argparse.Namespace:
    load_env(".env.dataproc")
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
        default=os.environ.get("RESULTS_PATH", "results/error_pattern_wall_clock.json"),
        help="Destination for the JSON results file (local or gs://)",
    )
    return parser.parse_args()


def _time_run(label: str, spark: SparkSession, parquet_path: str) -> dict[str, Any]:
    """Run one API variant and return its wall-clock time in seconds."""
    print(f"[benchmark] starting {label} ...", flush=True)
    start = time.perf_counter()

    if label == "RDD":
        top_ep, err_freq = rdd_build(spark, parquet_path)
        top_ep.collect()
        err_freq.collect()
    elif label == "DataFrame":
        top_ep, err_freq = df_build(spark, parquet_path)
        top_ep.collect()
        err_freq.collect()
    elif label == "SQL":
        top_ep, err_freq = sql_build(spark, parquet_path, _VIEW_NAME)
        top_ep.collect()
        err_freq.collect()

    elapsed = time.perf_counter() - start
    print(f"[benchmark] {label} finished in {elapsed:.3f}s", flush=True)
    return {"api": label, "elapsed_sec": round(elapsed, 3)}


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

    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    runs = [
        _time_run("RDD", spark, args.parquet_path),
        _time_run("DataFrame", spark, args.parquet_path),
        _time_run("SQL", spark, args.parquet_path),
    ]

    results = {"query": "error_pattern_analysis", "runs": runs}
    payload = json.dumps(results, indent=2)
    print(payload, flush=True)

    _write_results(payload, args.output_path)


if __name__ == "__main__":
    main()
