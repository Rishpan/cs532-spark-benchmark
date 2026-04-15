''' This uses the RDD API to implement the per-host traffic profiling query. It reads the parquet file into an RDD, performs the necessary transformations and actions to compute the traffic profiles for each host, and then saves the results to an output file. The traffic profile includes metrics such as total requests, 
average response time, and request distribution by status code for each host. '''
''' We will compute the following metrics for each host:
1) Total number of requests per host, in descending order of frequency.
2) Total bytes sent per host, in descending order of total bytes.
3) Average bytes per request per host, in descending order of average bytes.
4) Error rate per host (percentage of requests that resulted in an error status code, i.e., 4xx and 5xx), in descending order of error rate.
5) Distinct endpoints accessed per host.
'''

from pyspark.sql import SparkSession

from src.rdd_utils import read_parquet_into_rdd
from src.rdd_utils import parse_row_to_tuple
from src.rdd_utils import get_hostname

from src.session import get_spark_session, load_env, require_env

COLS = ["client_ip", "request_path", "status_code", "response_bytes"]
TOP_N = 10

def build_queries(spark: SparkSession, parquet_path: str) -> dict:
    # First, read the parquet file into an RDD and parse it into tuples
    rdd = read_parquet_into_rdd(spark, parquet_path, COLS).map(parse_row_to_tuple)

    # Map to (client_ip, (requests_per_host, bytes_per_host, error_requests_per_host, {endpoint})) for counting total requests, bytes, and error requests per host
    # we use frozenset to make it hashable for reduceByKey
    mapped_rdd = rdd.map(lambda row: (
        row[0],  # client_ip is the key
        (1, row[3] if row[3] is not None else 0, 1 if 400 <= row[2] < 600 else 0, frozenset({row[1]}))
    ))

    # Reduce by key to aggregate the metrics for each host
    # Note set must be unioned togethre to get distinct endpoints
    aggregated_rdd = mapped_rdd.reduceByKey(lambda a, b: (
        a[0] + b[0],
        a[1] + b[1],
        a[2] + b[2],
        a[3] | b[3]
    ))

    out_master_rdd = aggregated_rdd.map(lambda x: (
        x[0],
        x[1][0],
        x[1][1],
        x[1][1] / x[1][0] if x[1][0] > 0 else 0, 
        x[1][2] / x[1][0] * 100 if x[1][0] > 0 else 0,
        len(x[1][3])
    ))

    return out_master_rdd

    # # 1) Total number of requests per host
    # # Map to (client_ip, 1) for counting total requests per host
    # requests_per_host = rdd.map(lambda row: (row[0], 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: -x[1]).cache()

    # # 2) Total bytes sent per host
    # bytes_per_host = rdd.map(lambda row: (row[0], row[3] if row[3] is not None else 0)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: -x[1])

    # # 3) Average bytes per request per host
    # # jOIN total bytes and total requests to compute average bytes per request
    # average_bytes_per_host = bytes_per_host.join(requests_per_host).map(lambda x: (x[0], x[1][0] / x[1][1])).sortBy(lambda x: -x[1])

    # # 4) Error rate per host (percentage of requests that resulted in an error status code, i.e., 4xx and 5xx)
    # error_counts = rdd.filter(lambda row: 400 <= row[2] < 600).map(lambda row: (row[0], 1)).reduceByKey(lambda a, b: a + b)
    # error_rate_per_host = error_counts.join(requests_per_host).map(lambda x: (x[0], x[1][0] / x[1][1] * 100)).sortBy(lambda x: -x[1])

    # # 5) Distinct endpoints accessed per host
    # # First, map to (client_ip, request_path)
    # # Then use distinct to get unique (client_ip, request_path) pairs, and map to (client_ip, 1) to count distinct endpoints per host
    # distinct_endpoints_per_host = rdd.map(lambda row: (row[0], row[1])).distinct().map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: -x[1])

    # return {
    #     "requests_per_host": requests_per_host,
    #     "bytes_per_host": bytes_per_host,
    #     "average_bytes_per_host": average_bytes_per_host,
    #     "error_rate_per_host": error_rate_per_host,
    #     "distinct_endpoints_per_host": distinct_endpoints_per_host
    # }
    


if __name__ == "__main__":
    load_env()
    parquet_path = require_env("OUTPUT_PARQUET_PATH")

    spark = get_spark_session()
    out_master_rdd = build_queries(spark, parquet_path)

    res = out_master_rdd.collect()
    print("Top hosts by total requests:")
    for r in sorted(res, key=lambda x: -x[1])[:TOP_N]:
        print(f"{get_hostname(r[0])}: requests={r[1]}, bytes={r[2]}")

    print("\nTop hosts by error rate:")
    for r in sorted(res, key=lambda x: -x[4])[:TOP_N]:
        print(f"{get_hostname(r[0])}: error_rate={r[4]:.2f}%")

    print("\nTop hosts by average bytes per request:")
    for r in sorted(res, key=lambda x: -x[3])[:TOP_N]:
        print(f"{get_hostname(r[0])}: avg_bytes={r[3]:.2f}")

    print("\nTop hosts by most endpoints accessed:")
    for r in sorted(res, key=lambda x: -x[5])[:TOP_N]:
        print(f"{get_hostname(r[0])}: distinct_endpoints={r[5]}")

    # Only print the first 10 results for each metric to avoid overwhelming the output

    # print("Total requests per host:")
    # for host, count in results["requests_per_host"].take(TOP_N):
    #     print(f"{get_hostname(host)}: {count}")

    # print("\nTotal bytes sent per host:")
    # for host, bytes_sent in results["bytes_per_host"].take(TOP_N):
    #     print(f"{get_hostname(host)}: {bytes_sent}")

    # print("\nAverage bytes per request per host:")
    # for host, avg_bytes in results["average_bytes_per_host"].take(TOP_N):
    #     print(f"{get_hostname(host)}: {avg_bytes:.2f}")

    # print("\nError rate per host:")
    # for host, error_rate in results["error_rate_per_host"].take(TOP_N):
    #     print(f"{get_hostname(host)}: {error_rate:.2f}%")

    # print("\nDistinct endpoints accessed per host:")
    # for host, distinct_endpoints in results["distinct_endpoints_per_host"].take(TOP_N):
    #     print(f"{get_hostname(host)}: {distinct_endpoints}")