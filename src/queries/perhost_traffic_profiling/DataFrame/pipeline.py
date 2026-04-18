''' This uses the DataFrame API to implement the per-host traffic profiling query. It reads the parquet file into a DataFrame, performs the necessary transformations and actions to compute the traffic profiles for each host. '''
''' We will compute the following metrics for each host:
1) Total number of requests per host
2) Total bytes sent per host
3) Average bytes per request per host
4) Error rate per host (percentage of requests that resulted in an error status code, i.e., 4xx and 5xx)
5) Distinct endpoints accessed per host.
'''

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.session import get_spark_session, load_env, require_env
from src.df_utils import read_parquet_into_df
from src.df_utils import get_hostname

TOP_N = 10

def build_queries(spark: SparkSession, parquet_path: str) -> DataFrame:
    # First, read the parquet file into a DataFrame
    df = read_parquet_into_df(spark, parquet_path)

    # Get all metrics in a singular aggregation to avoid multiple passes over the data
    metrics_per_host = df.groupBy("client_ip").agg(
        F.count("*").alias("total_requests"),
        F.sum("response_bytes").alias("total_bytes"),
        F.avg("response_bytes").alias("average_bytes"),
        (F.sum(F.when(F.col("status_code") >= 400, 1).otherwise(0)) /F.count("*") * 100).alias("error_rate"),
        F.count_distinct("request_path").alias("distinct_endpoints")
    )
    
    return metrics_per_host


if __name__ == "__main__":
    load_env()
    parquet_path = require_env("OUTPUT_PARQUET_PATH")

    spark = get_spark_session()
    metrics = build_queries(spark, parquet_path)

    res = metrics.collect()
    print("Top hosts by total requests:")
    for r in sorted(res, key=lambda x: -x['total_requests'])[:TOP_N]:
        print(f"{get_hostname(r['client_ip'])}: requests={r['total_requests']}, bytes={r['total_bytes']}")

    print("\nTop hosts by error rate:")
    for r in sorted(res, key=lambda x: -x['error_rate'])[:TOP_N]:
        print(f"{get_hostname(r['client_ip'])}: error_rate={r['error_rate']:.2f}%")
    
    print("\nTop hosts by average bytes per request:")
    for r in sorted(res, key=lambda x: -x['average_bytes'])[:TOP_N]:
        print(f"{get_hostname(r['client_ip'])}: avg_bytes={r['average_bytes']:.2f}")
    
    print("\nTop hosts by distinct endpoints accessed:")
    for r in sorted(res, key=lambda x: -x['distinct_endpoints'])[:TOP_N]:
        print(f"{get_hostname(r['client_ip'])}: distinct_endpoints={r['distinct_endpoints']}")