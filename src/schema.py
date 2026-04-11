"""Canonical schema for cleaned Zanbil access rows (Parquet)."""
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Written to Parquet. Omitted: remote_ident / remote_user (always `-` in this dataset),
# referer, user-agent (not needed per project README).
ACCESS_LOG_SCHEMA: StructType = StructType(
    [
        StructField("client_ip", StringType(), False),
        StructField("log_ts", TimestampType(), False),
        StructField("http_method", StringType(), False),
        StructField("request_path", StringType(), False),
        StructField("http_version", StringType(), True),
        StructField("status_code", IntegerType(), False),
        StructField("response_bytes", LongType(), True),
        StructField("log_date", DateType(), False),
    ]
)
