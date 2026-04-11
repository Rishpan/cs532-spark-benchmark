# A Comparative Analysis of PySpark's RDD, DataFrame, and SQL APIs on E-Commerce Web Server Logs
Benchmarking PySpark's RDD, DataFrame, and SQL APIs on 3.3GB of e-commerce web server logs. Compares execution time, shuffle volume, stages/tasks, and memory usage across abstraction levels to analyze Spark's Catalyst optimizer.

Group Members: Atharva Kale, Gnaneswarudu Kuna, Kwame Afriyie Osei-Tutu, Rishab Pangal

## Setup Instructions

1. Create and activate a virtual environment:
```bash
    python -m venv cs532-spark-benchmark
    source cs532-spark-benchmark/bin/activate
```
    > On Windows: `cs532-spark-benchmark\Scripts\activate`

2. Install all dependencies:
```bash
    pip install -r requirements.txt
```

