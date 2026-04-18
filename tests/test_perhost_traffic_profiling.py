"""
Asserts that RDD, DataFrame, and SQL implementations of perhost_traffic_profiling
produce identical output for the same input data.

Two test classes:
  TestOptimizedPipeline — compares pipeline.py across all three APIs.
    Result: single structure with [client_ip, total_requests, total_bytes, average_bytes, error_rate, distinct_endpoints]

  TestNaivePipeline — compares naive_pipeline.py across all three APIs.
    Result: dict with five keys, each a [client_ip, <metric>] structure.

RDD vs DF: both renamed to canonical col names so data values are compared.
DF vs SQL: actual col names preserved — naming mismatches surface as test failures.
"""

import src.queries.perhost_traffic_profiling.RDD.pipeline as rdd_pipeline
import src.queries.perhost_traffic_profiling.DataFrame.pipeline as df_pipeline
import src.queries.perhost_traffic_profiling.SQL.pipeline as sql_pipeline

import src.queries.perhost_traffic_profiling.RDD.naive_pipeline as rdd_naive
import src.queries.perhost_traffic_profiling.DataFrame.naive_pipeline as df_naive
import src.queries.perhost_traffic_profiling.SQL.naive_pipeline as sql_naive

from tests.helpers import to_pandas, assert_frames_equal

SQL_VIEW = "ptp_test_view"
SQL_NAIVE_VIEW = "ptp_naive_test_view"

OPTIMIZED_COLS = ["client_ip", "total_requests", "total_bytes", "average_bytes", "error_rate", "distinct_endpoints"]

# Canonical col names for each naive metric (used to normalize RDD results and
# to rename DF results for the RDD vs DF comparison).
NAIVE_COLS = {
    "requests_per_host": ["client_ip", "total_requests"],
    "bytes_per_host": ["client_ip", "total_bytes"],
    "average_bytes_per_host": ["client_ip", "average_bytes"],
    "error_rate_per_host": ["client_ip", "error_rate"],
    "distinct_endpoints_per_host": ["client_ip", "distinct_endpoints"],
}


class TestOptimizedPipeline:
    def test_rdd_matches_dataframe(self, spark, parquet_path):
        rdd_result = rdd_pipeline.build_queries(spark, parquet_path)
        df_result = df_pipeline.build_queries(spark, parquet_path)

        rdd_pdf = to_pandas(rdd_result, OPTIMIZED_COLS)
        df_pdf = to_pandas(df_result)

        assert_frames_equal(rdd_pdf, df_pdf.rename(columns=dict(zip(df_pdf.columns, OPTIMIZED_COLS))), key_cols=["client_ip"])

    def test_dataframe_matches_sql(self, spark, parquet_path):
        df_result = df_pipeline.build_queries(spark, parquet_path)
        sql_result = sql_pipeline.build_queries(spark, parquet_path, SQL_VIEW)
        spark.catalog.dropTempView(SQL_VIEW)

        df_pdf = to_pandas(df_result)
        sql_pdf = to_pandas(sql_result)

        assert_frames_equal(df_pdf, sql_pdf)


class TestNaivePipeline:
    def test_rdd_matches_dataframe(self, spark, parquet_path):
        rdd_results = rdd_naive.build_queries(spark, parquet_path)
        df_results = df_naive.build_queries(spark, parquet_path)

        for key, col_names in NAIVE_COLS.items():
            rdd_pdf = to_pandas(rdd_results[key], col_names)
            df_pdf = to_pandas(df_results[key])
            assert_frames_equal(rdd_pdf, df_pdf.rename(columns=dict(zip(df_pdf.columns, col_names))), key_cols=["client_ip"])

    def test_dataframe_matches_sql(self, spark, parquet_path):
        df_results = df_naive.build_queries(spark, parquet_path)
        sql_results = sql_naive.build_queries(spark, parquet_path, SQL_NAIVE_VIEW)
        spark.catalog.dropTempView(SQL_NAIVE_VIEW)

        for key in NAIVE_COLS:
            df_pdf = to_pandas(df_results[key])
            sql_pdf = to_pandas(sql_results[key])
            assert_frames_equal(df_pdf, sql_pdf)
