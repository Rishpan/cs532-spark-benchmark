"""
Dataproc entrypoint for the log-parsing preprocessing job.

Uses session.load_env(".env.dataproc") so that all require_env() calls in
session.py and the pipeline resolve from the Dataproc config file rather than
the local-only .env.

Submit via:
    gcloud dataproc jobs submit pyspark \
        gs://BUCKET/staging/run_log_parsing.py \
        --py-files gs://BUCKET/staging/src.zip \
        --cluster CLUSTER --region REGION
"""
from __future__ import annotations
import argparse
import os


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run log parsing with optional scale overrides.")
    parser.add_argument("--sample-percent", type=int, default=None)
    parser.add_argument("--output-parquet-path", default=None)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    # Both imports are deferred so os.environ is populated before any
    # module-level require_env() calls in session.py or pipeline.py execute.
    from src.session import load_env
    load_env(".env.dataproc")
    if args.sample_percent is not None:
        os.environ["RAW_LOG_SAMPLE_PERCENT"] = str(args.sample_percent)
    if args.output_parquet_path is not None:
        os.environ["OUTPUT_PARQUET_PATH"] = args.output_parquet_path

    from src.queries.log_parsing.pipeline import main as log_parsing_main
    log_parsing_main()


if __name__ == "__main__":
    main()
