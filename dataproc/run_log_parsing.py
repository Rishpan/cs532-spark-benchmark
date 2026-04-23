"""
Dataproc entrypoint for the log-parsing preprocessing job.

Uses session.load_env(".env.dataproc") so that all require_env() calls in
session.py and the pipeline resolve from the Dataproc config file rather than
the local-only .env.

Submit via:
    gcloud dataproc jobs submit pyspark \
        gs://BUCKET/staging/run_log_parsing.py \
        --py-files gs://BUCKET/staging/src.zip \
        --cluster CLUSTER --region REGION
"""
from __future__ import annotations


def main() -> None:
    # Both imports are deferred so os.environ is populated before any
    # module-level require_env() calls in session.py or pipeline.py execute.
    from src.session import load_env
    load_env(".env.dataproc")

    from src.queries.log_parsing.pipeline import main as log_parsing_main
    log_parsing_main()


if __name__ == "__main__":
    main()
