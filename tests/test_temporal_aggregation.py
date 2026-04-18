"""
Asserts that RDD, DataFrame, and SQL implementations of temporal_aggregation
produce identical output for the same input data.

Results compared:
  1) hourly_metrics -> [hour, total_requests, total_bytes, average_bytes, error_rate]
  2) daily_metrics  -> [date, total_requests]

"""

import src.queries.temporal_aggregation.RDD.pipeline as rdd_pipeline
import src.queries.temporal_aggregation.DataFrame.pipeline as df_pipeline
import src.queries.temporal_aggregation.SQL.pipeline as sql_pipeline

from tests.helpers import to_pandas, assert_frames_equal

SQL_VIEW = "ta_test_view"

HOURLY_COLS = ["hour", "total_requests", "total_bytes", "average_bytes", "error_rate"]
DAILY_COLS = ["date", "total_requests"]


def test_hourly_metrics_consistent(spark, parquet_path):
    rdd_hourly, _ = rdd_pipeline.build_queries(spark, parquet_path)
    df_hourly, _ = df_pipeline.build_queries(spark, parquet_path)
    sql_hourly, _ = sql_pipeline.build_queries(spark, parquet_path, SQL_VIEW)
    spark.catalog.dropTempView(SQL_VIEW)

    rdd_pdf = to_pandas(rdd_hourly, HOURLY_COLS)
    df_pdf = to_pandas(df_hourly)
    sql_pdf = to_pandas(sql_hourly)

    assert_frames_equal(rdd_pdf, df_pdf.rename(columns=dict(zip(df_pdf.columns, HOURLY_COLS))), key_cols=["hour"])
    assert_frames_equal(df_pdf, sql_pdf)


def test_daily_metrics_consistent(spark, parquet_path):
    _, rdd_daily = rdd_pipeline.build_queries(spark, parquet_path)
    _, df_daily = df_pipeline.build_queries(spark, parquet_path)
    _, sql_daily = sql_pipeline.build_queries(spark, parquet_path, SQL_VIEW)
    spark.catalog.dropTempView(SQL_VIEW)

    rdd_pdf = to_pandas(rdd_daily, DAILY_COLS)
    df_pdf = to_pandas(df_daily)
    sql_pdf = to_pandas(sql_daily)

    assert_frames_equal(rdd_pdf, df_pdf.rename(columns=dict(zip(df_pdf.columns, DAILY_COLS))), key_cols=["date"])
    assert_frames_equal(df_pdf, sql_pdf)
