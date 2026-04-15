# Code to read parquet files for RDD pipeline
import socket 

def read_parquet_into_df(spark_session, parquet_path):
    # Read the parquet file into a DataFrame (specifying columns is preferred as it avoids unnecessary data transfer)
    df = spark_session.read.parquet(parquet_path)
    return df

def get_hostname(ip):
    try:
        return socket.gethostbyaddr(ip)[0]
    except (socket.herror, socket.gaierror):
        return ip