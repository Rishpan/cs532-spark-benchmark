# This uses the SQL to implement the temporal aggregation query. It reads the parquet file into an DataFrame, creates a temporary view, and performs the necessary transformations and actions to compute the temporal aggregations, and then saves the results to an output file.
# We will look for multiple temporal aggregations, which includes:
# 1) Total requests per hour
# 2) Total requests per day
# 3) Average bytes per request per hour
# 4) Error rate per hour (errors/total)
# 5) Total bytes per hour

from pyspark.sql import SparkSession

from src.sql_utils import read_parquet_into_tmpview
from src.sql_utils import get_hostname
from src.session import get_spark_session, load_env, require_env

TOP_N = 10

def build_queries(spark: SparkSession, parquet_path: str, view_name: str) -> str:
    # First, read the parquet file into a temporary view
    read_parquet_into_tmpview(spark, parquet_path, view_name)

    sql_query_hourly = f"""
    SELECT
        HOUR(log_ts) AS hour,
        COUNT(*) AS total_requests,
        SUM(response_bytes) AS total_bytes,
        AVG(response_bytes) AS average_bytes,
        (SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS error_rate
    FROM {view_name}
    GROUP BY hour
    """

    sql_query_daily = f"""
    SELECT
        DATE(log_ts) AS day,
        COUNT(*) AS total_requests
    FROM {view_name}
    GROUP BY day
    """

    hourly_metrics = spark.sql(sql_query_hourly)
    daily_metrics = spark.sql(sql_query_daily)

    return hourly_metrics, daily_metrics

if __name__ == "__main__":
    load_env()
    parquet_path = require_env("OUTPUT_PARQUET_PATH")
    spark = get_spark_session()
    view_name = "zanbil_logs_view"

    hourly_metrics, daily_metrics = build_queries(spark, parquet_path, view_name)

    res1, res2 = hourly_metrics.collect(), daily_metrics.collect()

    print("Total requests per day:")
    for r in sorted(res2, key=lambda x: -x['total_requests'])[:TOP_N]:
        print(f"{r['day']}: requests={r['total_requests']}")
    
    print("\nHours by highest request volume:")
    for r in sorted(res1, key=lambda x: -x['total_requests'])[:TOP_N]:
        print(f"{r['hour']}:00 : requests={r['total_requests']}, bytes={r['total_bytes']}, avg_bytes={r['average_bytes']:.2f}")
    
    print("\nHours by highest error rate:")
    for r in sorted(res1, key=lambda x: -x['error_rate'])[:TOP_N]:
        print(f"{r['hour']}:00 : error_rate={r['error_rate']:.2f}%")
    
    # Delete views to free up resources
    spark.catalog.dropTempView(view_name)