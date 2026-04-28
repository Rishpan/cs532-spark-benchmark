# A Comparative Analysis of PySpark's RDD, DataFrame, and SQL APIs on E-Commerce Web Server Logs
## Project Description and Relevance
In this project, we aim to characterize the systems performance of batch-processing frameworks across different levels of abstraction. To achieve this goal, we will use the [Zanbil.ir E-commerce Web Server Access Logs dataset](https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs)[1] — approximately 10+ million requests totaling ~3.3 GB uncompressed (freely available on Kaggle). This dataset uses the Apache Combined Log Format, which contains the same core fields as the Common Log Format (host, timestamp, method, endpoint, status code, payload size) with two additional fields (Referer and User-Agent) that will be ignored during parsing. We will design a feature extraction pipeline consisting of several analytically meaningful queries — including log parsing, sessionization, per-host traffic profiling, error pattern analysis, and temporal aggregation — and implement each query three ways: using Spark's low-level RDD API, the DataFrame API, and Spark SQL. Right now, the plan for the project is to implement at least 4 out of 5 of these queries. The systems component of this project centers on understanding how Spark's query execution differs across these three abstraction levels, and why. The RDD API requires the programmer to manually manage data partitioning, join strategies, and caching, while the DataFrame and SQL interfaces delegate these decisions to Spark's Catalyst query optimizer. By implementing the same workload at all three levels, we can directly observe how Catalyst's automatic optimizations compare against hand-tuned RDD code. This connects directly to the course's coverage of the Spark execution model (Lecture 8), the RDD paper by Zaharia et al. [2], and the broader theme of evaluating system design tradeoffs.
To characterize performance, we will measure several systems-level metrics: wall-clock execution time, shuffle read/write volume, number of stages and tasks, and peak memory usage. 

Group Members: Atharva Kale, Gnaneswarudu Kuna, Kwame Afriyie Osei-Tutu, Rishab Pangal

## Setup Instructions

### It is highly recommended that you run this on WSL if you have a Windows machine! It's much easier.
1. Create and activate a virtual environment:

Windows (Powershell):
```powershell
    python -m venv cs532-spark-benchmark
    cs532-spark-benchmark\Scripts\Activate.ps1
```

Linux:
```bash
    python -m venv 532-spark-bmark-linux
    source 532-spark-bmark-linux/bin/activate
```

2. Install all dependencies:
```bash
    pip install -r requirements.txt
```

3. Install **Java 17** (required for PySpark in this repo). Set **`JAVA_HOME`** in **`.env`** if Spark cannot find `java` (see **`.env.example`**).

4. **Python 3.12+ requires PySpark 3.5.** `requirements.txt` is already pinned to `pyspark==3.5.*`. PySpark 3.3 (used on Dataproc 2.1) ships a `cloudpickle` that cannot serialize Python 3.12 bytecode, so local development uses 3.5 while the Dataproc cluster runs 3.3. All query APIs used in this repo are compatible with both versions.

Note: On WSL, it seems like PySpark requires Java 17. Run this to install it:
```bash
    sudo apt install openjdk-17-jdk -y
```

## Run preprocessing pipeline

Requires **Java 21**. Set **`JAVA_HOME`** in **`.env`** if needed.

1. Download the [Zanbil / web server access logs](https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs) dataset from Kaggle and place the uncompressed `access.log` at **`data/raw/access.log`** (or set **`RAW_LOG_PATH`** in the repo-root **`.env`**).
2. Edit **`.env`** — set **`SPARK_MASTER=local[N]`** to your CPU count, **`JAVA_HOME`** if Spark can't find Java, and **`RAW_LOG_SAMPLE_PERCENT`** (under 100 for a random subset). See **`.env.example`** for all variables.
3. From the **repository root**, with your venv activated:

```bash
python -m src.queries.log_parsing.pipeline
```

On success, Spark writes **Snappy Parquet** under **`data/processed/access_logs/`** (path from **`OUTPUT_PARQUET_PATH`**). The job prints raw line count, regex match count, cleaned row count, parse rates, then **read-back** row count, **schema**, and a **5-row sample**. If read-back count is **0**, the process exits with an error.

## Run benchmark locally

Requires the Parquet data from the preprocessing pipeline above.

From the **repository root**, with your venv activated:

```bash
python -m benchmark.wall_clock 
```

This runs the all queries using all three APIs (RDD, DataFrame, SQL) and writes wall-clock times to **`results/allqueries_wall_clock.json`**. Paths are read from **`.env`** (`OUTPUT_PARQUET_PATH`, `RESULTS_PATH`) — override them on the command line if needed:

```bash
python -m benchmark.wall_clock \
        --parquet-path data/processed/access_logs \
        --output-path results/allqueries_wall_clock.json
```

By default, wall-clock runs each query/API once. Set `WALL_CLOCK_NUM_RUNS` in `.env`/`.env.dataproc`, or override with:
```bash
python -m benchmark.wall_clock --num-runs 5
```
Output now includes:
- per-run rows (`run` index),
- a `summary` block with `avg_elapsed_sec` and `std_elapsed_sec` per API,
- metadata (`benchmark_id`, `scale_pct`, `generated_at_utc`).

For local-only runs, `--scale-pct` and `--benchmark-id` are optional:
```bash
python -m benchmark.wall_clock \
        --parquet-path data/processed/access_logs \
        --output-path results/allqueries_wall_clock.json \
        --scale-pct 25 \
        --benchmark-id local-test-001
```

To get all other metrics (fetched via /stages API endpoint), run:
```bash
python -m benchmark.stage_metrics
```
This runs the all queries using all three APIs (RDD, DataFrame, SQL) and writes all metrics to **`results/stage_metrics.json`**. Paths are read from **`.env`** (`OUTPUT_PARQUET_PATH`, `RESULTS_PATH`) — override them on the command line if needed:
```bash
python -m benchmark.stage_metrics \
        --parquet-path data/processed/access_logs \
        --output-path results/stage_metrics.json
```

By default, stage-metrics runs each query/API once. Set `STAGE_METRICS_NUM_RUNS` in `.env`/`.env.dataproc`, or override with:
```bash
python -m benchmark.stage_metrics --num-runs 5
```
Output now includes per-run rows (`run` index), a `summary` block with per-metric avg/std per API, and metadata (`benchmark_id`, `scale_pct`, `generated_at_utc`).

**Same as Dataproc in one Spark job (optional):** to run wall-clock and stage-metrics back-to-back in a single local session—matching what `make job-benchmark` does on the cluster—use:

```bash
python -m benchmark.run_benchmark \
        --parquet-path data/processed/access_logs \
        --wall-clock-output-path results/allqueries_wall_clock.json \
        --stage-metrics-output-path results/stage_metrics.json
```

Repeat counts use `--wall-clock-num-runs` and `--stage-metrics-num-runs` (or `WALL_CLOCK_NUM_RUNS` / `STAGE_METRICS_NUM_RUNS` in `.env`). Merged rollup paths (`--wall-clock-merged-output-path`, `--stage-metrics-merged-output-path`) are optional.

To get the plans for the different queries with Dataframe API and the lineages for the queries with the RDD API, run:
```bash
python -m benchmark.plans
```
This runs the all queries using all the RDD and DataFrame APIs and writes the plans/lineages to the **`results/plans`** directory. Paths are read from **`.env`** (`OUTPUT_PARQUET_PATH`, `RESULTS_PATH`) — override them on the command line if needed:
```bash
python -m benchmark.plans \
        --parquet-path data/processed/access_logs \
        --output-dir results/plans
```

`benchmark.plans` also writes `run_manifest.json` with `benchmark_id`, `scale_pct`, and timestamp metadata.
<!-- ## Mid-Project Goals, due by 4/15/2026 11:59 PM EST
- Set up shared GitHub repository and development environment (PySpark installed and tested on each member's machine)
- Download and preprocess the Zanbil.ir E-commerce Web Server Access Logs dataset; verify it loads correctly in PySpark
- Decide the cluster size (GCP/AWS)
- Design the query workload and implement for one API type (Spark RDDs)
- Determine the specific metrics for measuring system performance
- Draft and submit the milestone check-in report -->

## Running the Benchmark on GCP Dataproc

The `Makefile` orchestrates the full pipeline: packaging source code, staging artifacts to GCS, provisioning a Dataproc cluster, running the log-parsing preprocessing job and a **combined** benchmark job (wall-clock timing and `/stages` metrics in one Spark application), fetching results back locally, and tearing down the cluster.

### Prerequisites

- `gcloud` CLI installed and authenticated (`gcloud auth login && gcloud auth application-default login`)
- A GCP project with billing enabled
- The uncompressed `access.log` dataset available locally (see **Run preprocessing pipeline** above for the download link)

### One-time setup

1. **Copy the Dataproc config template and fill in your values:**

```bash
cp .env.dataproc.example .env.dataproc
# Edit .env.dataproc — set GCP_PROJECT, GCS_BUCKET, REGION, RAW_LOG_LOCAL, etc.
```

2. **Enable required GCP APIs** (Dataproc, Cloud Resource Manager):

```bash
make setup-services
```

3. **Grant the Dataproc Worker role** to the default Compute Engine service account:

```bash
make setup-iam
```

4. **Create the GCS bucket:**

```bash
make bucket-create
```

5. **Compress and upload the raw access log to GCS:**

```bash
make stage-raw-log
```

This compresses `access.log` to `staging/access.log.gz` locally (only if the source has changed), then uploads it to `gs://<GCS_BUCKET>/data/raw/access.log.gz`.

### Run the pipeline

Run each step in order (with required `SCALE_PCT`):

```bash
make cluster-create                    # provision the Dataproc cluster
make job-log-parsing SCALE_PCT=5       # parse+sample raw logs into scale-specific Parquet
make job-benchmark SCALE_PCT=5         # wall-clock + stage-metrics in one Dataproc job
make job-plans SCALE_PCT=5             # capture plans for this scale
make fetch-results SCALE_PCT=5         # fetch raw run artifacts + merge deduped rollups locally
make cluster-delete                    # tear down the cluster
```

To run multiple repetitions per query/API in Dataproc, pass Make vars explicitly on the `make` command line:
```bash
make job-benchmark SCALE_PCT=5 WALL_CLOCK_NUM_RUNS=2 STAGE_METRICS_NUM_RUNS=2
```
`job-benchmark` now fails fast if these run-count variables are omitted.

Scale behavior:
- Allowed scale values are controlled by `SCALE_PCTS` in the `Makefile`.
- Every scale writes sampled parquet to `gs://<bucket>/data/processed/access_logs/pct=<SCALE_PCT>/`.
- `make job-benchmark` and `make job-plans` exit before submitting unless that scale’s Parquet prefix contains Spark’s `_SUCCESS` marker (run `make job-log-parsing SCALE_PCT=…` first).
- Every benchmark run writes raw outputs to `gs://<bucket>/results/scaling/pct=<SCALE_PCT>/id=<BENCHMARK_ID>/`.
- `BENCHMARK_ID` auto-generates if omitted.

Merge behavior (two-side, idempotent):
- On cluster write: wall-clock and stage-metrics also upsert merged per-scale rollups in GCS.
- On `fetch-results`: local merged rollups are rebuilt/updated from downloaded raw artifacts without duplicates.
- Dedupe keys include `benchmark_id`, `scale_pct`, query, API, and run.

Local fetched artifacts are stored under **`results/scaling/pct=<SCALE_PCT>/`**.

Other useful targets:

| Target | Description |
|---|---|
| `make job-benchmark` | Submit combined wall-clock + stage-metrics benchmark (requires `SCALE_PCT`) |
| `make bucket-delete` | Delete the GCS bucket and all its contents |
| `make help` | Print all available targets and the prerequisite order |

---

## AI Usage

- This project used AI assistance (Claude via Cursor) to help design the implement the GCP Dataproc deployment pipeline, and debug compatibility issues across Spark, Java, and GCS. All AI-generated code was reviewed, tested, and integrated by the project team.

- AI assistance was used to help think of the individual metrics to implement for each query type. 

- AI assistance was used to help decide how to use the /stages API endpoint to fetch the desired system performance metrics

- AI assistance was used to refactor the code such that it would work locally and on GCP (i.e. writing to a file)

---

Citations
[1] Dabbas, E. (n.d.). Web server access logs. Kaggle. https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs
[2] Matei Zaharia, Mosharaf Chowdhury, Tathagata Das, Ankur Dave, Justin Ma, Murphy McCauly, Michael J. Franklin, Scott Shenker, & Ion Stoica (2012). Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing. In 9th USENIX Symposium on Networked Systems Design and Implementation (NSDI 12) (pp. 15–28). USENIX Association.
