"""Parse Apache Combined Log lines from a text DataFrame (single-column `value`)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Zanbil logs have a trailing extra quoted field after user-agent (e.g. "-"), making 3 quoted
# tail fields total. Accept 0, 2, or 3 trailing quoted fields; all are ignored (not stored).
_COMBINED_LINE = (
    r'^(\S+) (\S+) (\S+) \[([^\]]+)\] "([^"]*)" (\d{3}) (-|\d+)'
    r'(?: "[^"]*" "[^"]*"(?:\s+"[^"]*")?)?\s*$'
)


def read_raw_log_lines(spark: SparkSession, path: str) -> DataFrame:
    """Read raw access log as UTF-8 text lines."""
    return spark.read.option("encoding", "UTF-8").text(path)


def parse_combined_log_lines(lines: DataFrame) -> DataFrame:
    """
    Add parsed columns from `value`. Rows that do not match combined format still get columns
    (mostly null / empty); cleaning step drops invalid rows.
    """
    v = F.col("value")
    ip = F.regexp_extract(v, _COMBINED_LINE, 1)
    ident = F.regexp_extract(v, _COMBINED_LINE, 2)
    user = F.regexp_extract(v, _COMBINED_LINE, 3)
    ts_raw = F.regexp_extract(v, _COMBINED_LINE, 4)
    request = F.regexp_extract(v, _COMBINED_LINE, 5)
    status_str = F.regexp_extract(v, _COMBINED_LINE, 6)
    bytes_str = F.regexp_extract(v, _COMBINED_LINE, 7)

    method = F.regexp_extract(request, r"^(\S+)", 1)
    http_version = F.regexp_extract(request, r"(\S+)$", 1)
    path = F.regexp_extract(request, r"^\S+\s+(.+)\s+\S+$", 1)

    log_ts = F.to_timestamp(ts_raw, "dd/MMM/yyyy:HH:mm:ss Z")

    return lines.select(
        v.alias("raw_line"),
        F.when(v.rlike(_COMBINED_LINE), F.lit(True))
        .otherwise(F.lit(False))
        .alias("line_matched"),
        ip.alias("client_ip"),
        ident.alias("remote_ident"),
        user.alias("remote_user"),
        ts_raw.alias("ts_raw"),
        log_ts.alias("log_ts"),
        method.alias("http_method"),
        path.alias("request_path"),
        http_version.alias("http_version"),
        status_str.alias("status_str"),
        bytes_str.alias("bytes_str"),
    )
