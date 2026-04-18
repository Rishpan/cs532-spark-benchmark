import pytest

from src.session import get_spark_session, load_env, require_env


@pytest.fixture(scope="session")
def spark():
    load_env()
    return get_spark_session()


@pytest.fixture(scope="session")
def parquet_path(spark):
    return require_env("OUTPUT_PARQUET_PATH")
