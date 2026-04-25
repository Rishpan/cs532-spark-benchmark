"""Build SparkSession from a repo-root env file loaded via python-dotenv."""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _prepend_java_bin(java_home: str) -> None:
    """Put ``JAVA_HOME/bin`` first on PATH so PySpark finds ``java``."""
    bin_dir = Path(java_home).expanduser().resolve() / "bin"
    if not bin_dir.is_dir():
        return
    prefix = str(bin_dir)
    sep = os.pathsep
    path = os.environ.get("PATH", "")
    if prefix not in path.split(sep):
        os.environ["PATH"] = prefix + sep + path


def load_env(env_file: str = ".env") -> None:
    # On Dataproc, --files distributes files to the job working directory (CWD).
    # Locally, fall back to the repo root.
    cwd_path = Path(env_file)
    load_dotenv(cwd_path if cwd_path.exists() else repo_root() / env_file)


def require_env(name: str) -> str:
    """Return env value. Variable must exist (empty string is allowed)."""
    if name not in os.environ:
        raise ValueError(
            f"Environment variable {name} is not set. Define it in the repo-root env file."
        )
    return os.environ[name]


def get_spark_session() -> SparkSession:
    load_env()
    _prepend_java_bin(require_env("JAVA_HOME"))
    master = require_env("SPARK_MASTER")
    app_name = require_env("SPARK_APP_NAME")
    shuffle = require_env("SPARK_SHUFFLE_PARTITIONS")
    java_opts = require_env("SPARK_JAVA_OPTS").strip()

    b = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", shuffle)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.retainedStages", 1000)
        .config("spark.ui.retainedJobs", 1000) 
    )
    if java_opts:
        os.environ["JAVA_TOOL_OPTIONS"] = java_opts
        b = b.config("spark.driver.extraJavaOptions", java_opts).config(
            "spark.executor.extraJavaOptions", java_opts
        )
    return b.getOrCreate()
