# This uses SQL to implement the error pattern analysis query. It reads the parquet file into a temporary view, performs the necessary transformations and actions to compute the error patterns, and then saves the results to an output file.
# We will look for two things:
# 1) Most common error endpoints, sorted in descending order of frequency
# 2) Frequencies for each error status code (4xx and 5xx) + their frequencies relative to total requests. Sorted in descending order of frequency.

from pyspark.sql import SparkSession

from src.sql_utils import read_parquet_into_tmpview
from src.session import get_spark_session, load_env, require_env

TOP_N = 10

def build_queries(spark: SparkSession, parquet_path: str, view_name: str) -> tuple:
    read_parquet_into_tmpview(spark, parquet_path, view_name)

    total = spark.sql(f"SELECT COUNT(*) AS total FROM {view_name}").collect()[0]['total']

    # 1) Top N error endpoints
    top_endpoints_query = f"""
    SELECT
        request_path,
        COUNT(*) AS count
    FROM {view_name}
    WHERE status_code >= 400 AND status_code < 600
    GROUP BY request_path
    ORDER BY count DESC
    """

    # 2) Error frequencies by status code
    error_freq_query = f"""
    SELECT
        status_code,
        COUNT(*) AS count,
        COUNT(*) / {total} AS frequency
    FROM {view_name}
    WHERE status_code >= 400 AND status_code < 600
    GROUP BY status_code
    ORDER BY frequency DESC
    """

    top_endpoints = spark.sql(top_endpoints_query)
    error_freq_rate = spark.sql(error_freq_query)

    return top_endpoints, error_freq_rate


if __name__ == "__main__":

    load_env()
    parquet_path = require_env("OUTPUT_PARQUET_PATH")
    spark = get_spark_session()
    view_name = "zanbil_logs_view"

    top_endpoints, error_freq_rate = build_queries(spark, parquet_path, view_name)

    print("Top error endpoints:")
    for r in top_endpoints.take(TOP_N):
        print(f"{r['request_path']}: {r['count']}")

    print("\nError frequencies by status code:")
    for r in error_freq_rate.take(TOP_N):
        print(f"{r['status_code']}: {r['count']} ({r['frequency']:.4%})")

    spark.catalog.dropTempView(view_name)
