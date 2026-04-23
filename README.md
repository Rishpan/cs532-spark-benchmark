# A Comparative Analysis of PySpark's RDD, DataFrame, and SQL APIs on E-Commerce Web Server Logs
Benchmarking PySpark's RDD, DataFrame, and SQL APIs on 3.3GB of e-commerce web server logs. Compares execution time, shuffle volume, stages/tasks, and memory usage across abstraction levels to analyze Spark's Catalyst optimizer.

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

3. Install **Java 21** (required for PySpark in this repo). Set **`JAVA_HOME`** in **`.env`** if Spark cannot find `java` (see **`.env.example`**).

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

## Project Description and Relevance
In this project, we aim to characterize the systems performance of batch-processing frameworks across different levels of abstraction. To achieve this goal, we will use the [Zanbil.ir E-commerce Web Server Access Logs dataset](https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs)[1] — approximately 10+ million requests totaling ~3.3 GB uncompressed (freely available on Kaggle). This dataset uses the Apache Combined Log Format, which contains the same core fields as the Common Log Format (host, timestamp, method, endpoint, status code, payload size) with two additional fields (Referer and User-Agent) that will be ignored during parsing. We will design a feature extraction pipeline consisting of several analytically meaningful queries — including log parsing, sessionization, per-host traffic profiling, error pattern analysis, and temporal aggregation — and implement each query three ways: using Spark's low-level RDD API, the DataFrame API, and Spark SQL. Right now, the plan for the project is to implement at least 4 out of 5 of these queries. The systems component of this project centers on understanding how Spark's query execution differs across these three abstraction levels, and why. The RDD API requires the programmer to manually manage data partitioning, join strategies, and caching, while the DataFrame and SQL interfaces delegate these decisions to Spark's Catalyst query optimizer. By implementing the same workload at all three levels, we can directly observe how Catalyst's automatic optimizations compare against hand-tuned RDD code. This connects directly to the course's coverage of the Spark execution model (Lecture 8), the RDD paper by Zaharia et al. [2], and the broader theme of evaluating system design tradeoffs.
To characterize performance, we will measure several systems-level metrics: wall-clock execution time, shuffle read/write volume, number of stages and tasks, and peak memory usage. 

## Mid-Project Goals, due by 4/15/2026 11:59 PM EST
- Set up shared GitHub repository and development environment (PySpark installed and tested on each member's machine)
- Download and preprocess the Zanbil.ir E-commerce Web Server Access Logs dataset; verify it loads correctly in PySpark
- Decide the cluster size (GCP/AWS)
- Design the query workload and implement for one API type (Spark RDDs)
- Determine the specific metrics for measuring system performance
- Draft and submit the milestone check-in report

## Running the Benchmark on GCP Dataproc

The `Makefile` orchestrates the full pipeline: packaging source code, staging artifacts to GCS, provisioning a Dataproc cluster, running the log-parsing preprocessing job and the wall-clock benchmark job, fetching results back locally, and tearing down the cluster.

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

Run each step in order:

```bash
make cluster-create       # provision the Dataproc cluster
make job-log-parsing      # parse raw logs → Parquet in GCS
make job-benchmark        # run RDD / DataFrame / SQL wall-clock benchmark
make fetch-results        # copy results JSON to results/
make cluster-delete       # tear down the cluster
```

Results are saved to **`results/error_pattern_wall_clock.json`**.

Other useful targets:

| Target | Description |
|---|---|
| `make bucket-delete` | Delete the GCS bucket and all its contents |
| `make help` | Print all available targets and the prerequisite order |

---

## AI Usage

This project used AI assistance (Claude via Cursor) to help design the implement the GCP Dataproc deployment pipeline, and debug compatibility issues across Spark, Java, and GCS. All AI-generated code was reviewed, tested, and integrated by the project team.

---

Citations
[1] Dabbas, E. (n.d.). Web server access logs. Kaggle. https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs
[2] Matei Zaharia, Mosharaf Chowdhury, Tathagata Das, Ankur Dave, Justin Ma, Murphy McCauly, Michael J. Franklin, Scott Shenker, & Ion Stoica (2012). Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing. In 9th USENIX Symposium on Networked Systems Design and Implementation (NSDI 12) (pp. 15–28). USENIX Association.
