# This uses the DataFrame API to implement the temporal aggregation query. It reads the parquet file into a DataFrame, performs the necessary transformations and actions to compute the temporal aggregations, and then saves the results to an output file.
# We will look for multiple temporal aggregations, which includes:
# 1) Total requests per hour
# 2) Total requests per day
# 3) Average bytes per request per hour
# 4) Error rate per hour (errors/total)
# 5) Total bytes per hour

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.session import get_spark_session, load_env, require_env
from src.df_utils import read_parquet_into_df
from src.df_utils import get_hostname

TOP_N = 10

def build_queries(spark: SparkSession, parquet_path: str) -> tuple:
    # First, read the parquet file into a DataFrame
    df = read_parquet_into_df(spark, parquet_path)

    # Group by hour and compute the metrics
    # Aliases are total_requests, total_bytes, average_bytes, error_rate

    metrics_per_hour = df.groupBy(F.hour("log_ts").alias("hour")).agg(
        F.count("*").alias("total_requests"),
        F.sum("response_bytes").alias("total_bytes"),
        F.avg("response_bytes").alias("average_bytes"),
        (F.sum(F.when(F.col("status_code") >= 400, 1).otherwise(0)) / F.count("*") * 100).alias("error_rate")
    )

    # Group by day and compute total requests per day
    metrics_per_day = df.groupBy(F.to_date("log_ts").alias("date")).agg(
        F.count("*").alias("total_requests")
    )

    return metrics_per_hour, metrics_per_day

if __name__ == "__main__":
    load_env()
    parquet_path = require_env("OUTPUT_PARQUET_PATH")

    spark = get_spark_session()
    hourly_metrics, daily_metrics = build_queries(spark, parquet_path)
    
    res1, res2 = hourly_metrics.collect(), daily_metrics.collect()

    print("Total requests per day:")
    for r in sorted(res2, key=lambda x: -x['total_requests'])[:TOP_N]:
        print(f"Date: {r['date']}, requests={r['total_requests']}")
    
    print("\nHours by highest request volume:")
    for r in sorted(res1, key=lambda x: -x['total_requests'])[:TOP_N]:
        print(f"{r['hour']}:00 : requests={r['total_requests']}, bytes={r['total_bytes']}, avg_bytes={r['average_bytes']:.2f}")
    
    print("\nHours by highest error rate:")
    for r in sorted(res1, key=lambda x: -x['error_rate'])[:TOP_N]:
        print(f"{r['hour']}:00 : error_rate={r['error_rate']:.2f}%")