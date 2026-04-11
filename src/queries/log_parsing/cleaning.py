"""Filter and cast parsed combined-log columns."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_ALLOWED_METHODS = (
    "GET",
    "POST",
    "HEAD",
    "PUT",
    "DELETE",
    "OPTIONS",
    "PATCH",
    "CONNECT",
    "TRACE",
)


def clean_access_logs(parsed: DataFrame, strict_http_method: bool = False) -> DataFrame:
    """
    Drop non-matching lines, validate status, normalize bytes, add `log_date`.
    If strict_http_method, drop rows whose method is not in a small HTTP verb set.
    """
    status = F.col("status_str").cast("int")
    bytes_clean = F.when(F.col("bytes_str") == "-", F.lit(None)).otherwise(
        F.col("bytes_str").cast("long")
    )

    df = (
        parsed.where(F.col("line_matched"))
        .where(status.isNotNull() & (status >= 100) & (status < 600))
        .where(F.col("log_ts").isNotNull())
        .where(F.trim(F.col("client_ip")) != "")
        .where(F.trim(F.col("http_method")) != "")
        .where(F.trim(F.col("request_path")) != "")
    )

    if strict_http_method:
        df = df.where(
            F.upper(F.trim(F.col("http_method"))).isin(list(_ALLOWED_METHODS))
        )

    http_ver = F.trim(F.col("http_version"))

    df = df.select(
        F.trim(F.col("client_ip")).alias("client_ip"),
        F.col("log_ts"),
        F.upper(F.trim(F.col("http_method"))).alias("http_method"),
        F.trim(F.col("request_path")).alias("request_path"),
        F.when(http_ver == "", F.lit(None)).otherwise(http_ver).alias("http_version"),
        status.alias("status_code"),
        bytes_clean.alias("response_bytes"),
        F.to_date(F.col("log_ts")).alias("log_date"),
    )

    return df
