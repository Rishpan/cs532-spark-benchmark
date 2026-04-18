"""
Asserts that RDD, DataFrame, and SQL implementations of error_pattern_analysis
produce identical output for the same input data.

Results compared:
  1) top_endpoints   -> [request_path, count]
  2) error_freq_rate -> [status_code, count, frequency]
"""

import src.queries.error_pattern_analysis.RDD.pipeline as rdd_pipeline
import src.queries.error_pattern_analysis.DataFrame.pipeline as df_pipeline
import src.queries.error_pattern_analysis.SQL.pipeline as sql_pipeline

from tests.helpers import to_pandas, assert_frames_equal

TOP_ENDPOINTS_COLS = ["request_path", "count"]
ERROR_FREQ_COLS = ["status_code", "count", "frequency"]
SQL_VIEW = "epa_test_view"


def test_top_endpoints_consistent(spark, parquet_path):
    rdd_top, _ = rdd_pipeline.build_queries(spark, parquet_path)
    df_top, _ = df_pipeline.build_queries(spark, parquet_path)
    sql_top, _ = sql_pipeline.build_queries(spark, parquet_path, SQL_VIEW)
    spark.catalog.dropTempView(SQL_VIEW)

    rdd_pdf = to_pandas(rdd_top, TOP_ENDPOINTS_COLS)
    df_pdf = to_pandas(df_top)
    sql_pdf = to_pandas(sql_top)

    # RDD vs DF: rename DF to canonical so data values are compared
    assert_frames_equal(rdd_pdf, df_pdf.rename(columns=dict(zip(df_pdf.columns, TOP_ENDPOINTS_COLS))), key_cols=["request_path"])
    # DF vs SQL: actual column names preserved — naming mismatches surface as failures
    assert_frames_equal(df_pdf, sql_pdf)


def test_error_freq_rate_consistent(spark, parquet_path):
    _, rdd_freq = rdd_pipeline.build_queries(spark, parquet_path)
    _, df_freq = df_pipeline.build_queries(spark, parquet_path)
    _, sql_freq = sql_pipeline.build_queries(spark, parquet_path, SQL_VIEW)
    spark.catalog.dropTempView(SQL_VIEW)

    rdd_pdf = to_pandas(rdd_freq, ERROR_FREQ_COLS)
    df_pdf = to_pandas(df_freq)
    sql_pdf = to_pandas(sql_freq)

    assert_frames_equal(rdd_pdf, df_pdf.rename(columns=dict(zip(df_pdf.columns, ERROR_FREQ_COLS))), key_cols=["status_code"])
    assert_frames_equal(df_pdf, sql_pdf)
