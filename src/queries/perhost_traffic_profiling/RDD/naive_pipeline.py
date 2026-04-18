''' This uses the RDD API to implement the per-host traffic profiling query. It reads the parquet file into an RDD, performs the necessary transformations and actions to compute the traffic profiles for each host. '''
''' We will compute the following metrics for each host:
1) Total number of requests per host
2) Total bytes sent per host
3) Average bytes per request per host
4) Error rate per host (percentage of requests that resulted in an error status code, i.e., 4xx and 5xx)
5) Distinct endpoints accessed per host.
'''

# NOTE: This is an unoptimized version of the pipeline that uses the RDD API. 
# Instead of grouping, it performs each metric separately and then executes individual actions to collect the results.

from pyspark.sql import SparkSession

from src.rdd_utils import read_parquet_into_rdd
from src.rdd_utils import parse_row_to_tuple

from src.session import get_spark_session, load_env, require_env

COLS = ["client_ip", "request_path", "status_code", "response_bytes"]
TOP_N = 10

def build_queries(spark: SparkSession, parquet_path: str) -> dict:
    # First, read the parquet file into an RDD and parse it into tuples
    rdd = read_parquet_into_rdd(spark, parquet_path, COLS).map(parse_row_to_tuple)

    # 1) Total number of requests per host
    # Map to (client_ip, 1) for counting total requests per host
    requests_per_host = rdd.map(lambda row: (row[0], int(1))).reduceByKey(lambda a, b: a + b)

    # 2) Total bytes sent per host
    bytes_per_host = rdd.map(lambda row: (row[0], row[3] if row[3] is not None else 0)).reduceByKey(lambda a, b: a + b)

    # 3) Average bytes per request per host
    # jOIN total bytes and total requests to compute average bytes per request
    average_bytes_per_host = bytes_per_host.join(requests_per_host).map(lambda x: (x[0], x[1][0] / x[1][1]))

    # 4) Error rate per host (percentage of requests that resulted in an error status code, i.e., 4xx and 5xx)
    # leftOuterJoin from requests_per_host so hosts with zero errors are included (error_count defaults to 0)
    error_counts = rdd.filter(lambda row: 400 <= row[2] < 600).map(lambda row: (row[0], int(1))).reduceByKey(lambda a, b: a + b)
    error_rate_per_host = requests_per_host.leftOuterJoin(error_counts).map(lambda x: (x[0], (x[1][1] or 0) / x[1][0] * 100))

    # 5) Distinct endpoints accessed per host
    # First, map to (client_ip, request_path)
    # Then use distinct to get unique (client_ip, request_path) pairs, and map to (client_ip, 1) to count distinct endpoints per host
    distinct_endpoints_per_host = rdd.map(lambda row: (row[0], row[1])).distinct().map(lambda x: (x[0], int(1))).reduceByKey(lambda a, b: a + b)

    return {
        "requests_per_host": requests_per_host,
        "bytes_per_host": bytes_per_host,
        "average_bytes_per_host": average_bytes_per_host,
        "error_rate_per_host": error_rate_per_host,
        "distinct_endpoints_per_host": distinct_endpoints_per_host
    }

if __name__ == "__main__":
    load_env()
    parquet_path = require_env("OUTPUT_PARQUET_PATH")
    spark = get_spark_session()

    results = build_queries(spark, parquet_path)
    # Dont need to print anything here
    for k in results:
        results[k].count() # Just call count() to trigger the computation