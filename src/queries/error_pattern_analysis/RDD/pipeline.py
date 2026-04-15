# This uses the RDD API to implement the error pattern analysis query. It reads the parquet file into an RDD, performs the necessary transformations and actions to compute the error patterns, and then saves the results to an output file.
# We will look for two things: 
# 1) Most common error endpoints, sorted in descending order of frequency
# 2) Frequencies for each error status code (4xx and 5xx) + their frequencies relative to total requests. Sorted in descending order of frequency.

from pyspark.sql import SparkSession

from src.rdd_utils import read_parquet_into_rdd
from src.rdd_utils import parse_row_to_tuple

from src.session import get_spark_session, load_env, require_env

COLS = ["request_path", "status_code"]
TOP_N = 10

def build_queries(spark: SparkSession, parquet_path: str) -> dict:
    rdd = read_parquet_into_rdd(spark, parquet_path, COLS).map(parse_row_to_tuple)

    total = rdd.count()

    # Map to ((endpoint, status_code), 1) for counting so key is (endpoint, status_code) and value is count of 1 for each error occurrence
    error_master_rdd = rdd.filter(lambda row: 400 <= row[1] < 600).map(
        lambda row: ((row[0], row[1]), 1)).reduceByKey(
        lambda a, b: a + b
    )
    
    # # 1) Top N error endpoints
    # Map to (endpoint, count for that (endpoint, status_code)) and then reduce by endpoint to get total count for each endpoint, 
    # then sort by count in descending order
    top_endpoints = error_master_rdd.map(lambda x: (x[0][0], x[1])).reduceByKey(lambda a, b: a + b).sortBy(lambda x: -x[1])

    # 2) Error frequencies by status code
    # Map to (status_code, count for that (endpoint, status_code)) and then reduce by status code to get total count for each status code, 
    # then sort by count in descending order. 
    # Then map to (status_code, count, frequency) where frequency is count / total requests, and sort by frequency in descending order
    error_frequencies = error_master_rdd.map(lambda x: (x[0][1], x[1])).reduceByKey(lambda a, b: a + b)
    error_freq_rate = error_frequencies.map(lambda x: (x[0], x[1], x[1] / total if total > 0 else 0)).sortBy(lambda x: -x[2])

    return top_endpoints, error_freq_rate
    

if __name__ == "__main__":

    load_env()
    parquet_path = require_env("OUTPUT_PARQUET_PATH")

    spark = get_spark_session()
    top_endpoints, error_freq_rate = build_queries(spark, parquet_path)

    print("Top error endpoints:")
    for endpoint, count in top_endpoints.take(TOP_N):
        print(f"{endpoint}: {count}")
    
    print("\nError frequencies by status code:")
    for status_code, count, frequency in error_freq_rate.take(TOP_N):
        print(f"{status_code}: {count} ({frequency:.4%})")