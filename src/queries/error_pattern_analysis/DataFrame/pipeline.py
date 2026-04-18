# This uses the DataFrame API to implement the error pattern analysis query. It reads the parquet file into a DataFrame, performs the necessary transformations and actions to compute the error patterns, and then saves the results to an output file.
# We will look for two things:
# 1) Most common error endpoints, sorted in descending order of frequency
# 2) Frequencies for each error status code (4xx and 5xx) + their frequencies relative to total requests. Sorted in descending order of frequency.

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.session import get_spark_session, load_env, require_env
from src.df_utils import read_parquet_into_df

TOP_N = 10

def build_queries(spark: SparkSession, parquet_path: str) -> tuple:
    df = read_parquet_into_df(spark, parquet_path)

    total = df.count()

    error_df = df.filter((F.col("status_code") >= 400) & (F.col("status_code") < 600))

    # 1) Top N error endpoints: group by request_path, count occurrences, sort descending
    top_endpoints = error_df.groupBy("request_path").agg(
        F.count("*").alias("count")
    ).orderBy(F.col("count").desc())

    # 2) Error frequencies by status code: group by status_code, count, compute rate relative to total
    error_freq_rate = error_df.groupBy("status_code").agg(
        F.count("*").alias("count")
    ).withColumn(
        "frequency", F.col("count") / total if total > 0 else F.lit(0.0)
    ).orderBy(F.col("frequency").desc())

    return top_endpoints, error_freq_rate


if __name__ == "__main__":

    load_env()
    parquet_path = require_env("OUTPUT_PARQUET_PATH")

    spark = get_spark_session()
    top_endpoints, error_freq_rate = build_queries(spark, parquet_path)

    print("Top error endpoints:")
    for r in top_endpoints.take(TOP_N):
        print(f"{r['request_path']}: {r['count']}")

    print("\nError frequencies by status code:")
    for r in error_freq_rate.take(TOP_N):
        print(f"{r['status_code']}: {r['count']} ({r['frequency']:.4%})")
