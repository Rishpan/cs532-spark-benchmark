# This uses the RDD API to implement the temporal aggregation query. It reads the parquet file into an RDD, performs the necessary transformations and actions to compute the temporal aggregations, and then saves the results to an output file.
# We will look for multiple temporal aggregations, which includes:
# 1) Total requests per hour
# 2) Total requests per day
# 3) Average bytes per request per hour
# 4) Error rate per hour (errors/total)
# 5) Total bytes per hour
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from src.rdd_utils import read_parquet_into_rdd
from src.rdd_utils import parse_row_to_tuple
from src.session import get_spark_session, load_env, require_env

COLS = ["log_ts", "response_bytes", "status_code"]
TOP_N = 10

def build_queries(spark: SparkSession, parquet_path: str) -> dict:
    # First, read the parquet file into an RDD and parse it into tuples
    rdd = read_parquet_into_rdd(spark, parquet_path, COLS).map(parse_row_to_tuple)

    # map to (hour, (1, bytes, 1 if error/0 if not error))
    mapped_rdd = rdd.map(lambda row: (
        datetime.fromtimestamp(row[0], tz=timezone.utc).hour,
        (1, row[1] if row[1] is not None else 0, 1 if 400 <= row[2] < 600 else 0)
    ))

    # reduce by key (hour) to get (hour, (total_requests, total_bytes, total_errors))
    aggregated_rdd = mapped_rdd.reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]
        )
    )

    out_hour_master_rdd = aggregated_rdd.map(lambda x: (
        x[0],
        x[1][0],
        x[1][1],
        x[1][1] / x[1][0] if x[1][0] > 0 else 0, 
        x[1][2] / x[1][0] * 100 if x[1][0] > 0 else 0
    ))

    daily_master_rdd = rdd.map(lambda row: (datetime.fromtimestamp(row[0], tz=timezone.utc).date(), 1)).reduceByKey(lambda a, b: a + b)

    return out_hour_master_rdd, daily_master_rdd
    # 
    # Note: Timestamps are datetime.date objects

    # # Map to (hour, 1) for counting total requests per hour
    # requests_per_hour = rdd.map(lambda row: (datetime.fromtimestamp(row[0], tz=timezone.utc).hour, 1)). \
    #     reduceByKey(lambda a, b: a + b).sortBy(lambda x: -x[1])
    
    # # Map to (day, 1) for counting total requests per day
    # requests_per_day = rdd.map(lambda row: (datetime.fromtimestamp(row[0], tz=timezone.utc).date(), 1)). \
    #     reduceByKey(lambda a, b: a + b).sortBy(lambda x: -x[1])

    # # Group by hour and sum response bytes for bytes transferred per hour
    # bytes_per_hour = rdd.map(lambda row: (datetime.fromtimestamp(row[0], tz=timezone.utc).hour, row[1] if row[1] is not None else 0)). \
    #     reduceByKey(lambda a, b: a + b).sortBy(lambda x: -x[1])
    
    # # Error rate per hour: we need to compute total requests and error requests per hour
    # # Then join them and compute error rate as error_requests / total_requests
    # total_requests_per_hour = rdd.map(lambda row: (datetime.fromtimestamp(row[0], tz=timezone.utc).hour, 1)).reduceByKey(lambda a, b: a + b)
    # error_requests_per_hour = rdd.filter(lambda row: 400 <= row[2] < 600).map(lambda row: (datetime.fromtimestamp(row[0], tz=timezone.utc).hour, 1)).reduceByKey(lambda a, b: a + b)
    # error_rate_per_hour = total_requests_per_hour.join(error_requests_per_hour).map(lambda x: (x[0], x[1][1] / x[1][0] * 100)).sortBy(lambda x: -x[1])

    # return {
    #     "requests_per_hour": requests_per_hour,
    #     "requests_per_day": requests_per_day,
    #     "bytes_per_hour": bytes_per_hour,
    #     "error_rate_per_hour": error_rate_per_hour
    # }

if __name__ == "__main__":
    load_env()
    parquet_path = require_env("OUTPUT_PARQUET_PATH")

    spark = get_spark_session()
    hourly_rdd, daily_rdd = build_queries(spark, parquet_path)
    # execute both RDDs and print results
    res1, res2 = hourly_rdd.collect(), daily_rdd.collect()

    print("Total requests per day:")
    for r in sorted(res2, key=lambda x: -x[1])[:TOP_N]:
        print(f"Date: {r[0]}, requests={r[1]}")
    
    print("\nHours by highest request volume:")
    for r in sorted(res1, key=lambda x: -x[1])[:TOP_N]:
        print(f"{r[0]}:00 : requests={r[1]}, bytes={r[2]}, avg_bytes={r[3]:.2f}")
    
    print("\nHours by highest error rate:")
    for r in sorted(res1, key=lambda x: -x[4])[:TOP_N]:
        print(f"{r[0]}:00 : error_rate={r[4]:.2f}%")
    
    # results = build_queries(spark, parquet_path)

    # # Only print the first 10 results for each metric to avoid overwhelming the output

    # print("Total requests per hour:")
    # for hour, count in results["requests_per_hour"].take(TOP_N):
    #     print(f"{hour}:00 : {count}")

    # print("\nTotal requests per day:")
    # for day, count in results["requests_per_day"].take(TOP_N):
    #     print(f"{day}: {count}")
    
    # # This is already computed in requests_per_hour, so we can just take the first element of the sorted RDD
    # hour, count = results["requests_per_hour"].first()
    # print(f"\nHour with most request volume: {hour}:00 with {count} requests")

    # print("\nBytes transferred per hour:")
    # for hour, total_bytes in results["bytes_per_hour"].take(TOP_N):
    #     print(f"{hour}:00 : {total_bytes} bytes")
    
    # print("\nError rate per hour:")
    # for hour, error_rate in results["error_rate_per_hour"].take(TOP_N):
    #     print(f"{hour}:00 : {error_rate:.4f}% error rate")
