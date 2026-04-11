"""
Preprocess Zanbil combined-format access log → Parquet under OUTPUT_PARQUET_PATH.

Place Kaggle `access.log` (uncompressed) at RAW_LOG_PATH (see repo-root `.env`).
`RAW_LOG_SAMPLE_PERCENT` / `RAW_LOG_SAMPLE_SEED` for approximate subsampling.
Run from repository root: `python -m src.queries.log_parsing.pipeline`
"""
from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import functions as F

from src.schema import ACCESS_LOG_SCHEMA
from src.session import get_spark_session, load_env, require_env

from .cleaning import clean_access_logs
from .parsing import parse_combined_log_lines, read_raw_log_lines


def main() -> None:
    load_env()

    raw_path   = Path(require_env("RAW_LOG_PATH"))
    out_dir    = Path(require_env("OUTPUT_PARQUET_PATH"))
    partitions = int(require_env("OUTPUT_PARTITIONS"))
    by_date    = require_env("PARTITION_BY_LOG_DATE").lower() == "true"
    strict     = require_env("STRICT_HTTP_METHOD").lower() == "true"
    sample_pct = float(require_env("RAW_LOG_SAMPLE_PERCENT"))
    sample_seed = int(require_env("RAW_LOG_SAMPLE_SEED"))

    if not raw_path.is_file():
        print(f"ERROR: RAW_LOG_PATH not a file: {raw_path}", file=sys.stderr)
        sys.exit(1)
    out_dir.parent.mkdir(parents=True, exist_ok=True)

    spark = get_spark_session()

    lines = read_raw_log_lines(spark, str(raw_path))
    if sample_pct < 100:
        lines = lines.sample(withReplacement=False, fraction=sample_pct / 100, seed=sample_seed)
        print(f"--- sampling ~{sample_pct}% (seed={sample_seed}) ---", flush=True)
    lines.cache()
    raw_n = lines.count()

    parsed = parse_combined_log_lines(lines)
    parsed.cache()
    matched_n = parsed.where(F.col("line_matched")).count()

    cleaned = clean_access_logs(parsed, strict_http_method=strict)
    cleaned = cleaned.select([f.name for f in ACCESS_LOG_SCHEMA.fields])
    cleaned = cleaned.repartition(partitions)
    cleaned.cache()
    cleaned_n = cleaned.count()

    writer = cleaned.write.mode("overwrite").option("compression", "snappy")
    if by_date:
        writer = writer.partitionBy("log_date")
    writer.parquet(str(out_dir))

    print(f"raw_lines={raw_n} regex_matched={matched_n} cleaned={cleaned_n}")
    if raw_n:
        print(f"parse_rate={cleaned_n / raw_n:.4f}  regex_match_rate={matched_n / raw_n:.4f}")

    verify = spark.read.parquet(str(out_dir))
    verify_n = verify.count()
    print(f"read_back={verify_n}")
    if verify_n == 0:
        print("ERROR: read-back row count is 0", file=sys.stderr)
        sys.exit(1)
    verify.printSchema()
    verify.show(5, truncate=False)

    lines.unpersist()
    parsed.unpersist()
    cleaned.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
