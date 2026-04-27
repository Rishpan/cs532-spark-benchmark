"""Sessionization using the RDD API.

A new session starts when the gap between consecutive requests
from the same client_ip exceeds SESSION_TIMEOUT_SECS (default: 1800s / 30 min).

Metrics computed:
  1) Total session count per IP (descending)
  2) Average session duration (seconds) per IP (descending)
  3) Average requests per session per IP (descending)
  4) Top 10 longest individual sessions (by duration)
"""

from datetime import datetime
from pyspark.sql import SparkSession

from src.rdd_utils import read_parquet_into_rdd
from src.session import get_spark_session, load_env, require_env

SESSION_TIMEOUT_SECS = 1800  # 30 minutes
COLS = ["client_ip", "log_ts"]


def _assign_sessions(timestamps: list, timeout: int) -> list[dict]:
    """
    Given a sorted list of timestamps for one IP, return a list of session dicts:
      {session_id, start, end, request_count, duration_secs}
    """
    sorted_ts = sorted(timestamps)
    sessions = []
    session_start = sorted_ts[0]
    session_count = 1
    prev = sorted_ts[0]

    for ts in sorted_ts[1:]:
        gap = (ts - prev).total_seconds()
        if gap > timeout:
            sessions.append({
                "start": session_start,
                "end": prev,
                "request_count": session_count,
                "duration_secs": (prev - session_start).total_seconds(),
            })
            session_start = ts
            session_count = 1
        else:
            session_count += 1
        prev = ts

    # Close the last session
    sessions.append({
        "start": session_start,
        "end": prev,
        "request_count": session_count,
        "duration_secs": (prev - session_start).total_seconds(),
    })
    return sessions


def run(spark: SparkSession, parquet_path: str, timeout: int = SESSION_TIMEOUT_SECS) -> dict:
    rdd = read_parquet_into_rdd(spark, parquet_path, COLS)

    # (client_ip, log_ts)
    pairs = rdd.map(lambda row: (row[0], row[1]))

    # Group all timestamps per IP, then assign sessions
    # sessions_rdd: (client_ip, [session_dict, ...])
    sessions_rdd = (
        pairs
        .groupByKey()
        .mapValues(lambda timestamps: _assign_sessions(list(timestamps), timeout))
    )

    # # 1) Session count per IP
    # session_counts = (
    #     sessions_rdd
    #     .mapValues(len)
    #     .sortBy(lambda x: x[1])  # type: ignore[arg-type]
    # )

    # # 2) Average session duration per IP
    # avg_duration = (
    #     sessions_rdd
    #     .mapValues(lambda s: sum(x["duration_secs"] for x in s) / len(s))
    #     .sortBy(lambda x: x[1])  # type: ignore[arg-type]
    # )

    # # 3) Average requests per session per IP
    # avg_requests = (
    #     sessions_rdd
    #     .mapValues(lambda s: sum(x["request_count"] for x in s) / len(s))
    #     .sortBy(lambda x: x[1])  # type: ignore[arg-type]
    # )

    # # 4) Top 10 longest individual sessions (client_ip, duration_secs)
    # flat_sessions = sessions_rdd.flatMap(
    #     lambda kv: [(kv[0], s["duration_secs"], s["request_count"]) for s in kv[1]]
    # )
    # top_sessions = flat_sessions.sortBy(lambda x: x[1]).take(10)  # type: ignore[arg-type]

    # Combine into one RDD of (client_ip, {session_count, avg_duration, avg_requests})
    # Mirrors implementations in DataFrame and SQL pipelines
    per_ip_rdd = sessions_rdd.mapValues(lambda s: {
        "session_count": len(s),
        "avg_duration": sum(x["duration_secs"] for x in s) / len(s),
        "avg_requests": sum(x["request_count"] for x in s) / len(s),
    })

    # return {
    #     "session_counts":  session_counts.collect(),
    #     "avg_duration":    avg_duration.collect(),
    #     "avg_requests":    avg_requests.collect(),
    #     "top_sessions":    top_sessions,
    # }

    return sessions_rdd, per_ip_rdd

if __name__ == "__main__":
    load_env()
    parquet_path = require_env("OUTPUT_PARQUET_PATH")
    spark = get_spark_session()
    sessions_rdd, per_ip_rdd = run(spark, parquet_path)
    sessions_rdd.count()
    per_ip_rdd.count()
    
    # print("Session count per IP (top 10):")
    # for ip, count in results["session_counts"][:10]:
    #     print(f"  {ip}: {count} sessions")

    # print("\nAvg session duration per IP, seconds (top 10):")
    # for ip, dur in results["avg_duration"][:10]:
    #     print(f"  {ip}: {dur:.1f}s")

    # print("\nAvg requests per session per IP (top 10):")
    # for ip, req in results["avg_requests"][:10]:
    #     print(f"  {ip}: {req:.1f}")

    # print("\nTop 10 longest individual sessions:")
    # for ip, dur, reqs in results["top_sessions"]:
    #     print(f"  {ip}: {dur:.1f}s, {reqs} requests")


