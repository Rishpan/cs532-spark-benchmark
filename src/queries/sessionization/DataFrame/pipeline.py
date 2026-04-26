"""Sessionization using the DataFrame API.

A new session starts when the gap between consecutive requests
from the same client_ip exceeds SESSION_TIMEOUT_SECS (default: 1800s / 30 min).

Session boundaries are detected with a lag() window function rather than
a Python loop, so Catalyst can optimise the execution plan.

Metrics computed:
  1) Total session count per IP (descending)
  2) Average session duration (seconds) per IP (descending)
  3) Average requests per session per IP (descending)
  4) Top 10 longest individual sessions (by duration)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from src.session import get_spark_session, load_env, require_env
from src.df_utils import read_parquet_into_df

SESSION_TIMEOUT_SECS = 1800  # 30 minutes
TOP_N = 10


def build_queries(spark: SparkSession, parquet_path: str, timeout: int = SESSION_TIMEOUT_SECS) -> tuple[DataFrame, DataFrame]:
    df = read_parquet_into_df(spark, parquet_path).select("client_ip", "log_ts")

    # Window ordered by timestamp within each IP
    w_ordered = Window.partitionBy("client_ip").orderBy("log_ts")

    # Compute gap from the previous request for the same IP
    df = df.withColumn("prev_ts", F.lag("log_ts").over(w_ordered))
    df = df.withColumn(
        "gap_secs",
        F.when(F.col("prev_ts").isNull(), None).otherwise(
            F.unix_timestamp("log_ts") - F.unix_timestamp("prev_ts")
        ),
    )

    # Flag the first request of each new session (null prev_ts = very first request)
    df = df.withColumn(
        "is_new_session",
        F.when(
            F.col("prev_ts").isNull() | (F.col("gap_secs") > timeout), 1
        ).otherwise(0),
    )

    # Cumulative sum of new-session flags gives a monotonically increasing
    # session ID within each IP partition
    w_cumulative = Window.partitionBy("client_ip").orderBy("log_ts").rowsBetween(
        Window.unboundedPreceding, 0
    )
    df = df.withColumn("session_id", F.sum("is_new_session").over(w_cumulative))

    # Per-session metrics: one row per (client_ip, session_id)
    sessions = df.groupBy("client_ip", "session_id").agg(
        F.count("*").alias("request_count"),
        (
            F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))
        ).alias("duration_secs"),
    ).cache()

    # Per-IP metrics: aggregate over all sessions for each IP
    per_ip = sessions.groupBy("client_ip").agg(
        F.count("session_id").alias("session_count"),
        F.avg("duration_secs").alias("avg_duration_secs"),
        F.avg("request_count").alias("avg_requests_per_session"),
    )

    return sessions, per_ip


if __name__ == "__main__":
    load_env()
    parquet_path = require_env("OUTPUT_PARQUET_PATH")
    spark = get_spark_session()

    sessions, per_ip = build_queries(spark, parquet_path)

    per_ip_res = per_ip.collect()
    sessions_res = sessions.collect()

    print("Session count per IP (top 10):")
    for r in sorted(per_ip_res, key=lambda x: -x["session_count"])[:TOP_N]:
        print(f"  {r['client_ip']}: {r['session_count']} sessions")

    print("\nAvg session duration per IP, seconds (top 10):")
    for r in sorted(per_ip_res, key=lambda x: -x["avg_duration_secs"])[:TOP_N]:
        print(f"  {r['client_ip']}: {r['avg_duration_secs']:.1f}s")

    print("\nAvg requests per session per IP (top 10):")
    for r in sorted(per_ip_res, key=lambda x: -x["avg_requests_per_session"])[:TOP_N]:
        print(f"  {r['client_ip']}: {r['avg_requests_per_session']:.1f}")

    print("\nTop 10 longest individual sessions:")
    for r in sorted(sessions_res, key=lambda x: -x["duration_secs"])[:TOP_N]:
        print(f"  {r['client_ip']} (session {r['session_id']}): {r['duration_secs']:.1f}s, {r['request_count']} requests")
