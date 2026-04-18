from __future__ import annotations

from typing import Optional

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame


def to_pandas(result, col_names: Optional[list] = None) -> pd.DataFrame:
    """Convert a Spark RDD or DataFrame to a pandas DataFrame.

    col_names: if provided, rename columns positionally (used to assign canonical
               names to RDD results which have no column metadata). For Spark
               DataFrames, omit col_names to preserve the actual output column names
               so that naming mismatches between implementations surface as failures.
    """
    if isinstance(result, SparkDataFrame):
        pdf = result.toPandas()
        if col_names is not None:
            pdf.columns = col_names
    else:
        rows = result.collect()
        pdf = pd.DataFrame(rows, columns=col_names)
    return pdf


def assert_frames_equal(df1: pd.DataFrame, df2: pd.DataFrame, key_cols: Optional[list] = None):
    """Sort both frames and assert they are equal (with float tolerance).

    key_cols: column names to sort by. When None, each frame is sorted by its own
              first column — this allows DF vs SQL comparisons to proceed even when
              the key column has different names (the name mismatch is then caught
              by assert_frame_equal).
    """
    sort_key1: list = key_cols if key_cols is not None else [df1.columns[0]]
    sort_key2: list = key_cols if key_cols is not None else [df2.columns[0]]
    df1 = df1.sort_values(by=sort_key1).reset_index(drop=True)
    df2 = df2.sort_values(by=sort_key2).reset_index(drop=True)
    pd.testing.assert_frame_equal(df1, df2, check_like=True, check_exact=False, rtol=1e-5, check_dtype=False)
