# This uses the RDD API to implement the temporal aggregation query. It reads the parquet file into an RDD, performs the necessary transformations and actions to compute the temporal aggregations, and then saves the results to an output file.
# We will look for multiple temporal aggregations, such as:
# 1) Total requests per hour, in descending order of frequency.
# 2) Total requests per day, in descending order of frequency.
# 3) Hour with most request volume, along with the request count.
# 4) Bytes transferred per hour, in descending order of total bytes.
# 5) Error rate per hour (errors/total), in descending order of error rate.

from pyspark.sql import SparkSession
from src.rdd_utils import read_parquet_into_rdd
from src.rdd_utils import parse_row_to_tuple
from src.session import get_spark_session, load_env, require_env

COLS = ["log_ts", "response_bytes", "status_code"]

def run(spark: SparkSession, parquet_path: str) -> dict:
    # First, read the parquet file into an RDD and parse it into tuples
    rdd = read_parquet_into_rdd(spark, parquet_path, COLS).map(parse_row_to_tuple).cache()

    # Note: Timestamps are datetime.date objects

    # Map to (hour, 1) for counting total requests per hour
    requests_per_hour = rdd.map(lambda row: (row[0].hour, 1)). \
        reduceByKey(lambda a, b: a + b).sortBy(lambda x: -x[1])
    
    # Map to (day, 1) for counting total requests per day
    requests_per_day = rdd.map(lambda row: (row[0].date(), 1)). \
        reduceByKey(lambda a, b: a + b).sortBy(lambda x: -x[1])
    
    # Find hour with most request volume
    # This is already computed in requests_per_hour, so we can just take the first element
    hour_with_most_requests = requests_per_hour.first() 

    # Group by hour and sum response bytes for bytes transferred per hour
    bytes_per_hour = rdd.map(lambda row: (row[0].hour, row[1] if row[1] is not None else 0)). \
        reduceByKey(lambda a, b: a + b).sortBy(lambda x: -x[1])
    
    # Error rate per hour: we need to compute total requests and error requests per hour
    # Then join them and compute error rate as error_requests / total_requests
    total_requests_per_hour = rdd.map(lambda row: (row[0].hour, 1)).reduceByKey(lambda a, b: a + b)
    error_requests_per_hour = rdd.filter(lambda row: 400 <= row[2] < 600).map(lambda row: (row[0].hour, 1)).reduceByKey(lambda a, b: a + b)
    error_rate_per_hour = total_requests_per_hour.join(error_requests_per_hour).map(lambda x: (x[0], x[1][1] / x[1][0])).sortBy(lambda x: -x[1])

    return {
        "requests_per_hour": requests_per_hour.collect(),
        "requests_per_day": requests_per_day.collect(),
        "hour_with_most_requests": hour_with_most_requests,
        "bytes_per_hour": bytes_per_hour.collect(),
        "error_rate_per_hour": error_rate_per_hour.collect()
    }

if __name__ == "__main__":
    load_env()
    parquet_path = require_env("OUTPUT_PARQUET_PATH")

    spark = get_spark_session()
    results = run(spark, parquet_path)

    print("Total requests per hour:")
    for hour, count in results["requests_per_hour"]:
        print(f"{hour}:00 : {count}")

    print("\nTotal requests per day:")
    for day, count in results["requests_per_day"]:
        print(f"{day}: {count}")
    
    hour, count = results["hour_with_most_requests"]
    print(f"\nHour with most request volume: {hour}:00 with {count} requests")

    print("\nBytes transferred per hour:")
    for hour, total_bytes in results["bytes_per_hour"]:
        print(f"{hour}:00 : {total_bytes} bytes")
    
    print("\nError rate per hour:")
    for hour, error_rate in results["error_rate_per_hour"]:
        print(f"{hour}:00 : {error_rate:.4%}% error rate")