''' This uses SQL to implement the per-host traffic profiling query. It reads the parquet file into an DF, makes a temporray view, performs the necessary transformations and actions to compute the traffic profiles for each host. '''
''' We will compute the following metrics for each host:
1) Total number of requests per host
2) Total bytes sent per host
3) Average bytes per request per host
4) Error rate per host (percentage of requests that resulted in an error status code, i.e., 4xx and 5xx)
5) Distinct endpoints accessed per host.
'''

# NOTE: This is an unoptimized version of the pipeline that uses SQL. 
# Instead of grouping, it performs each metric separately and then executes individual actions to collect the results.

from pyspark.sql import SparkSession

from src.sql_utils import read_parquet_into_tmpview
from src.sql_utils import get_hostname
from src.session import get_spark_session, load_env, require_env

TOP_N = 10

def build_queries(spark: SparkSession, parquet_path: str, view_name: str) -> dict:
    # First, read the parquet file into a temporary view
    read_parquet_into_tmpview(spark, parquet_path, view_name)

    # 1) Total number of requests per host
    sql_query_n_requests = f"""
    SELECT client_ip, COUNT(*) AS total_requests
    FROM {view_name}
    GROUP BY client_ip 
    """

    # 2) Total bytes sent per host
    sql_query_total_bytes = f"""
    SELECT client_ip, SUM(response_bytes) AS total_bytes
    FROM {view_name}
    GROUP BY client_ip 
    """

    # 3) Average bytes per request per host
    sql_query_avg_bytes = f"""
    SELECT client_ip, AVG(response_bytes) AS average_bytes
    FROM {view_name}
    GROUP BY client_ip 
    """

    # 4) Error rate per host (percentage of requests that resulted in an error status code, i.e., 4xx and 5xx)
    sql_query_error_rate = f"""
    SELECT client_ip, (SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS error_rate
    FROM {view_name}
    GROUP BY client_ip 
    """

    # 5) Distinct endpoints accessed per host
    sql_query_distinct_endpoints = f"""
    SELECT client_ip, COUNT(DISTINCT request_path) AS distinct_endpoints
    FROM {view_name}
    GROUP BY client_ip 
    """

    requests_per_host = spark.sql(sql_query_n_requests)
    bytes_per_host = spark.sql(sql_query_total_bytes)
    average_bytes_per_host = spark.sql(sql_query_avg_bytes)
    error_rate_per_host = spark.sql(sql_query_error_rate)
    distinct_endpoints_per_host = spark.sql(sql_query_distinct_endpoints)

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
    view_name = "zanbil_logs_view"

    results = build_queries(spark, parquet_path, view_name)

    # Don't need to print all results
    for k in results:
        results[k].count()  # Just call count() to trigger the computation