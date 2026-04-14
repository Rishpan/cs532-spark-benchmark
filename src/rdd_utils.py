# Code to read parquet files for RDD pipeline
import socket 
def read_parquet_into_rdd(spark_session, parquet_path, columns=None):
    # Read the parquet file into a RDD (specifying columns is preferred as it avoids unnecessary data transfer)
    if columns:
        df = spark_session.read.parquet(parquet_path).select(*columns)
    else:
        df = spark_session.read.parquet(parquet_path)
    return df.rdd

def parse_row_to_tuple(row):
    # Convert a Row object to a tuple
    return tuple(row)

def get_hostname(ip):
    try:
        return socket.gethostbyaddr(ip)[0]
    except (socket.herror, socket.gaierror):
        return ip