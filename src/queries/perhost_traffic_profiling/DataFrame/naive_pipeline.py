''' This uses the DataFrame API to implement the per-host traffic profiling query. It reads the parquet file into a DataFrame, performs the necessary transformations and actions to compute the traffic profiles for each host. '''
''' We will compute the following metrics for each host:
1) Total number of requests per host
2) Total bytes sent per host
3) Average bytes per request per host
4) Error rate per host (percentage of requests that resulted in an error status code, i.e., 4xx and 5xx)
5) Distinct endpoints accessed per host.
'''

# NOTE: This is an unoptimized version of the pipeline that uses the DataFrame API. 
# Instead of grouping, it performs each metric separately and then executes individual actions to collect the results.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.session import get_spark_session, load_env, require_env
from src.df_utils import read_parquet_into_df

TOP_N = 10


def build_queries(spark: SparkSession, parquet_path: str) -> dict:
    df = read_parquet_into_df(spark, parquet_path)

    # 1) Total number of requests per host
    requests_per_host = df.groupBy("client_ip").count().orderBy("count", ascending=False)

    # 2) Total bytes sent per host
    bytes_per_host = df.groupBy("client_ip").agg(F.sum("response_bytes").alias("total_bytes")).orderBy("total_bytes", ascending=False)

    # 3) Average bytes per request per host
    average_bytes_per_host = df.groupBy("client_ip").agg(F.avg("response_bytes").alias("average_bytes")).orderBy("average_bytes", ascending=False)

    # 4) Error rate per host (percentage of requests that resulted in an error status code, i.e., 4xx and 5xx)
    error_rate_per_host = df.groupBy("client_ip").agg(
        (F.sum(F.when(F.col("status_code") >= 400, 1).otherwise(0)) /F.count("*") * 100).alias("error_rate")
    ).orderBy("error_rate", ascending=False)

    # 5) Distinct endpoints accessed per host
    distinct_endpoints_per_host = df.groupBy("client_ip").agg(F.countDistinct("request_path").alias("distinct_endpoints")).orderBy("distinct_endpoints", ascending=False)


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

    # Don't need to print all results
    for k in results:
        results[k].count()  # Just call count() to trigger the computation