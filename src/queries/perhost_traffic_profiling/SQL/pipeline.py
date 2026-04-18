''' This uses SQL to implement the per-host traffic profiling query. It reads the parquet file into an DF, makes a temporray view, performs the necessary transformations and actions to compute the traffic profiles for each host. '''
''' We will compute the following metrics for each host:
1) Total number of requests per host
2) Total bytes sent per host
3) Average bytes per request per host
4) Error rate per host (percentage of requests that resulted in an error status code, i.e., 4xx and 5xx)
5) Distinct endpoints accessed per host.
'''

from pyspark.sql import SparkSession, DataFrame

from src.sql_utils import read_parquet_into_tmpview
from src.sql_utils import get_hostname
from src.session import get_spark_session, load_env, require_env

TOP_N = 10

def build_queries(spark: SparkSession, parquet_path: str, view_name: str) -> DataFrame:
    # First, read the parquet file into a temporary view
    read_parquet_into_tmpview(spark, parquet_path, view_name)

    sql_query = f"""
    SELECT 
        client_ip,
        COUNT(*) AS total_requests,
        SUM(response_bytes) AS total_bytes,
        AVG(response_bytes) AS average_bytes,
        (SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS error_rate,
        COUNT(DISTINCT request_path) AS distinct_endpoints
    FROM {view_name}
    GROUP BY client_ip
    """

    metrics_per_host = spark.sql(sql_query)

    return metrics_per_host

if __name__ == "__main__":
    load_env()
    parquet_path = require_env("OUTPUT_PARQUET_PATH")
    spark = get_spark_session()
    view_name = "zanbil_logs_view"

    metrics = build_queries(spark, parquet_path, view_name)

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
    
    # Drop the temporary view after we're done
    spark.catalog.dropTempView(view_name)