# Code to read parquet files for RDD pipeline
import socket 
from pyspark.sql import functions as F
def read_parquet_into_rdd(spark_session, parquet_path, columns=None):
    # Read the parquet file into a RDD (specifying columns is preferred as it avoids unnecessary data transfer)
    df = spark_session.read.parquet(parquet_path)
    if columns:
        df = df.select(*columns)
    if "log_ts" in (columns or df.columns):
        df = df.withColumn("log_ts", F.unix_timestamp(F.col("log_ts")))
    return df.rdd

def parse_row_to_tuple(row):
    # Convert a Row object to a tuple
    return tuple(row)

def get_hostname(ip):
    try:
        return socket.gethostbyaddr(ip)[0]
    except (socket.herror, socket.gaierror):
        return ip