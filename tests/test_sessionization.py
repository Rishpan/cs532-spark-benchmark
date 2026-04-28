"""
Asserts that RDD, DataFrame, and SQL implementations of sessionization
produce identical output for the same input data.

Results compared:
  1) sessions -> per-session rows: [client_ip, request_count, duration_secs]
                 (session_id is implementation-internal; RDD has none and
                 DF/SQL generate independent monotone IDs)
  2) per_ip   -> per-IP aggregates:
                 [client_ip, session_count, avg_duration_secs, avg_requests_per_session]
"""

import src.queries.sessionization.RDD.pipeline as rdd_pipeline
import src.queries.sessionization.DataFrame.pipeline as df_pipeline
import src.queries.sessionization.SQL.pipeline as sql_pipeline

from tests.helpers import to_pandas, assert_frames_equal

SQL_VIEW = "sess_test_view"

SESSIONS_COLS = ["client_ip", "request_count", "duration_secs"]
PER_IP_COLS = ["client_ip", "session_count", "avg_duration_secs", "avg_requests_per_session"]


def _drop_sql_views(spark):
    for v in (SQL_VIEW, "sessions_raw", "sessions_with_id"):
        spark.catalog.dropTempView(v)


def test_sessions_consistent(spark, parquet_path):
    rdd_sessions, _ = rdd_pipeline.build_queries(spark, parquet_path)
    df_sessions, _ = df_pipeline.build_queries(spark, parquet_path)
    sql_sessions, _ = sql_pipeline.build_queries(spark, parquet_path, SQL_VIEW)

    rdd_flat = rdd_sessions.flatMap(
        lambda kv: [(kv[0], s["request_count"], s["duration_secs"]) for s in kv[1]]
    )
    rdd_pdf = to_pandas(rdd_flat, SESSIONS_COLS)
    df_pdf_no_id = to_pandas(df_sessions.drop("session_id"))
    sql_pdf_no_id = to_pandas(sql_sessions.drop("session_id"))

    assert_frames_equal(
        rdd_pdf,
        df_pdf_no_id.rename(columns=dict(zip(df_pdf_no_id.columns, SESSIONS_COLS))),
        key_cols=["client_ip", "duration_secs", "request_count"],
    )
    df_pdf = to_pandas(df_sessions)
    sql_pdf = to_pandas(sql_sessions)
    assert_frames_equal(df_pdf, sql_pdf, key_cols=["client_ip", "session_id"])

    _drop_sql_views(spark)


def test_per_ip_consistent(spark, parquet_path):
    _, rdd_per_ip = rdd_pipeline.build_queries(spark, parquet_path)
    _, df_per_ip = df_pipeline.build_queries(spark, parquet_path)
    _, sql_per_ip = sql_pipeline.build_queries(spark, parquet_path, SQL_VIEW)

    rdd_flat = rdd_per_ip.map(
        lambda kv: (kv[0], kv[1]["session_count"], kv[1]["avg_duration"], kv[1]["avg_requests"])
    )
    rdd_pdf = to_pandas(rdd_flat, PER_IP_COLS)
    df_pdf = to_pandas(df_per_ip)
    sql_pdf = to_pandas(sql_per_ip)

    assert_frames_equal(
        rdd_pdf,
        df_pdf.rename(columns=dict(zip(df_pdf.columns, PER_IP_COLS))),
        key_cols=["client_ip"],
    )
    assert_frames_equal(df_pdf, sql_pdf)

    _drop_sql_views(spark)
