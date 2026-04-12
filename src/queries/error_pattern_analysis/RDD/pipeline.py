# This uses the RDD API to implement the error pattern analysis query. It reads the parquet file into an RDD, performs the necessary transformations and actions to compute the error patterns, and then saves the results to an output file.
# We will look for two things: 
# 1) Top N most common error endpoints, in descending order of frequency.
# 2) Frequencies for each error status code (4xx and 5xx), in descending order of frequency, along with the error rate (errors/total).

from pyspark.sql import SparkSession

from src.rdd_utils import read_parquet_into_rdd
from src.rdd_utils import parse_row_to_tuple

from src.session import get_spark_session, load_env, require_env

COLS = ["request_path", "status_code"]
TOP_N = 10

def run(spark: SparkSession, parquet_path: str) -> dict:
    rdd = read_parquet_into_rdd(spark, parquet_path, COLS).map(parse_row_to_tuple).cache()


    # Filter to only error rows (status_code >= 400)
    error_rdd_raw = rdd.filter(lambda row: row[1] >= 400).cache()
    # Map to (endpoint, 1) for counting so key is endpoint and value is count of 1 for each error occurrence
    error_rdd = error_rdd_raw.map(lambda row: (row[0], 1)) 

    # 1) Top N error endpoints
    top_endpoints = error_rdd.reduceByKey(lambda a, b: a + b).takeOrdered(TOP_N, key=lambda x: -x[1])

    # 2) Error frequencies by status code
    total = rdd.count()
    error_frequencies = error_rdd_raw.map(lambda row: (row[1], 1)).reduceByKey(lambda a, b: a + b)
    error_freq = error_frequencies.map(lambda x: (x[0], x[1], x[1] / total if total > 0 else 0)).sortBy(lambda x: -x[1])


    return {
        "top_error_endpoints": top_endpoints,
        "error_frequencies": error_freq.collect()
    }

if __name__ == "__main__":

    load_env()
    parquet_path = require_env("OUTPUT_PARQUET_PATH")

    spark = get_spark_session()
    results = run(spark, parquet_path)

    print("Top error endpoints:")
    for endpoint, count in results["top_error_endpoints"]:
        print(f"{endpoint}: {count}")

    print("\nError frequencies by status code:")
    for status_code, count, frequency in results["error_frequencies"]:
        print(f"{status_code}: {count} ({frequency:.4%})")