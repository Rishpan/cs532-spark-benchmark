# Code to read parquet files for RDD pipeline
import socket

from pyspark.sql import DataFrame 

def read_parquet_into_tmpview(spark_session, parquet_path, view_name):
    # Read the parquet file into a DataFrame (specifying columns is preferred as it avoids unnecessary data transfer)
    df: DataFrame = spark_session.read.parquet(parquet_path)
    df.createOrReplaceTempView(view_name)

    return view_name

def get_hostname(ip):
    try:
        return socket.gethostbyaddr(ip)[0]
    except (socket.herror, socket.gaierror):
        return ip