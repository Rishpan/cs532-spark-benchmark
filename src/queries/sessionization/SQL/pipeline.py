"""Sessionization using the SQL API
"""

from pyspark.sql import SparkSession, DataFrame

from src.sql_utils import read_parquet_into_tmpview
from src.session import get_spark_session, load_env, require_env

SESSION_TIMEOUT_SECS = 1800
TOP_N = 10


def build_queries(spark: SparkSession, parquet_path: str, view_name: str, timeout: int = SESSION_TIMEOUT_SECS) -> dict:
    read_parquet_into_tmpview(spark, parquet_path, view_name)

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW sessions_raw AS
        SELECT
            client_ip,
            log_ts,
            LAG(log_ts) OVER (PARTITION BY client_ip ORDER BY log_ts) AS prev_ts,
            CASE
                WHEN LAG(log_ts) OVER (PARTITION BY client_ip ORDER BY log_ts) IS NULL THEN 1
                WHEN (UNIX_TIMESTAMP(log_ts) - UNIX_TIMESTAMP(LAG(log_ts) OVER (PARTITION BY client_ip ORDER BY log_ts))) > {timeout} THEN 1
                ELSE 0
            END AS is_new_session
        FROM {view_name}
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW sessions_with_id AS
        SELECT
            client_ip,
            log_ts,
            SUM(is_new_session) OVER (PARTITION BY client_ip ORDER BY log_ts) AS session_id
        FROM sessions_raw
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW session_stats AS
        SELECT
            client_ip,
            session_id,
            MIN(log_ts) AS session_start,
            MAX(log_ts) AS session_end,
            COUNT(*) AS request_count,
            UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts)) AS duration_secs
        FROM sessions_with_id
        GROUP BY client_ip, session_id
    """)

    sessions = spark.sql("""
        SELECT
            client_ip,
            session_id,
            COUNT(*) AS request_count,
            UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts)) AS duration_secs
        FROM sessions_with_id
        GROUP BY client_ip, session_id
    """)

    # session_counts = spark.sql("""
    #     SELECT client_ip, COUNT(session_id) AS session_count
    #     FROM session_stats
    #     GROUP BY client_ip
    #     ORDER BY session_count DESC
    # """)

    # avg_duration = spark.sql("""
    #     SELECT client_ip, AVG(duration_secs) AS avg_duration_secs
    #     FROM session_stats
    #     GROUP BY client_ip
    #     ORDER BY avg_duration_secs DESC
    # """)

    # avg_requests = spark.sql("""
    #     SELECT client_ip, AVG(request_count) AS avg_requests_per_session
    #     FROM session_stats
    #     GROUP BY client_ip
    #     ORDER BY avg_requests_per_session DESC
    # """)

    # top_sessions = spark.sql("""
    #     SELECT client_ip, duration_secs, request_count
    #     FROM session_stats
    #     ORDER BY duration_secs DESC
    #     LIMIT 10
    # """)

    # Combined above into one query to mirror RDD and DataFrame implementations
    per_ip = spark.sql("""
        SELECT
            client_ip,
            COUNT(session_id) AS session_count,
            AVG(duration_secs) AS avg_duration_secs,
            AVG(request_count) AS avg_requests_per_session
        FROM (
            SELECT
                client_ip,
                session_id,
                COUNT(*) AS request_count,
                UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts)) AS duration_secs
            FROM sessions_with_id
            GROUP BY client_ip, session_id
        )
        GROUP BY client_ip
    """)

    return sessions, per_ip


if __name__ == "__main__":
    load_env()
    parquet_path = require_env("OUTPUT_PARQUET_PATH")
    spark = get_spark_session()
    view_name = "zanbil_logs_view"

    sessions, per_ip = build_queries(spark, parquet_path, view_name)
    sessions.count()
    per_ip.count()

    # print("Session count per IP (top 10):")
    # for r in results["session_counts"].limit(TOP_N).collect():
    #     print(f"  {r['client_ip']}: {r['session_count']} sessions")

    # print("\nAvg session duration per IP, seconds (top 10):")
    # for r in results["avg_duration"].limit(TOP_N).collect():
    #     print(f"  {r['client_ip']}: {r['avg_duration_secs']:.1f}s")

    # print("\nAvg requests per session per IP (top 10):")
    # for r in results["avg_requests"].limit(TOP_N).collect():
    #     print(f"  {r['client_ip']}: {r['avg_requests_per_session']:.1f}")

    # print("\nTop 10 longest individual sessions:")
    # for r in results["top_sessions"].collect():
    #     print(f"  {r['client_ip']}: {r['duration_secs']:.1f}s, {r['request_count']} requests")

    spark.catalog.dropTempView(view_name)
    spark.catalog.dropTempView("sessions_raw")
    spark.catalog.dropTempView("sessions_with_id")
    spark.catalog.dropTempView("session_stats")
