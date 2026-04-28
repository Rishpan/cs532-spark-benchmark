# ──────────────────────────────────────────────────────────────────────────────
# Configuration — all variables are read from .env.dataproc (gitignored).
# Copy .env.dataproc.example → .env.dataproc and fill in your values before
# running any target.
# ──────────────────────────────────────────────────────────────────────────────
include .env.dataproc

GCS_PREFIX    := gs://$(GCS_BUCKET)
GCS_STAGING   := $(GCS_PREFIX)/staging
GCS_RAW_LOG   := $(GCS_PREFIX)/data/raw/access.log.gz
GCS_PARQUET   := $(GCS_PREFIX)/data/processed/access_logs
GCS_RESULTS   := $(GCS_PREFIX)/results
SCALE_PCTS    ?= 5 10 25 50 100
BENCHMARK_ID  ?= $(shell date +%Y%m%d-%H%M%S)-pct$(SCALE_PCT)
GCS_PARQUET_SCALE := $(GCS_PARQUET)/pct=$(SCALE_PCT)
GCS_RESULTS_SCALE := $(GCS_RESULTS)/scaling/pct=$(SCALE_PCT)
GCS_RESULTS_RUN   := $(GCS_RESULTS_SCALE)/id=$(BENCHMARK_ID)
LOCAL_RESULTS_SCALE := results/scaling/pct=$(SCALE_PCT)
FETCH_BENCHMARK_ID ?=
WALL_CLOCK_NUM_RUNS ?= 1
STAGE_METRICS_NUM_RUNS ?= 1

.PHONY: setup-services setup-iam bucket-create bucket-delete cluster-create cluster-delete \
        job-log-parsing job-benchmark job-plans \
        stage-raw-log fetch-results help check-scale check-parquet-ready

# ──────────────────────────────────────────────────────────────────────────────
# Packaging — real file target, only re-zips when source files change
# ──────────────────────────────────────────────────────────────────────────────
PYTHON_SOURCES := $(shell find src/ benchmark/ dataproc/ -name '*.py')

staging/src.zip: $(PYTHON_SOURCES)
	mkdir -p staging
	zip -r staging/src.zip src/ benchmark/ dataproc/
	@echo "[package] staging/src.zip updated"

# ──────────────────────────────────────────────────────────────────────────────
# Services — enable required GCP APIs. Run once before setup-iam.
# Not part of the pipeline.
# ──────────────────────────────────────────────────────────────────────────────
setup-services:
	gcloud services enable dataproc.googleapis.com \
	  cloudresourcemanager.googleapis.com \
	  --project=$(GCP_PROJECT)
	gcloud config set storage/parallel_composite_upload_enabled False
	@echo "[services] required APIs enabled"

# ──────────────────────────────────────────────────────────────────────────────
# IAM — grant Dataproc Worker role to the default Compute service account.
# Run once after setup-services. Not part of the pipeline.
# ──────────────────────────────────────────────────────────────────────────────
setup-iam:
	gcloud projects add-iam-policy-binding $(GCP_PROJECT) \
	  --member="serviceAccount:$$(gcloud projects describe $(GCP_PROJECT) --format='value(projectNumber)')-compute@developer.gserviceaccount.com" \
	  --role="roles/dataproc.worker"
	@echo "[iam] Dataproc Worker role granted"

# ──────────────────────────────────────────────────────────────────────────────
# Bucket — create GCS bucket if it does not already exist
# ──────────────────────────────────────────────────────────────────────────────
bucket-create:
	gcloud storage buckets describe $(GCS_PREFIX) --project=$(GCP_PROJECT) > /dev/null 2>&1 || \
	  gcloud storage buckets create  $(GCS_PREFIX) --project=$(GCP_PROJECT) --location=$(REGION)
	@echo "[bucket] $(GCS_PREFIX) ready"

# Deletes the bucket and ALL its contents. Not part of any pipeline — run manually.
bucket-delete:
	gcloud storage rm --recursive $(GCS_PREFIX)
	@echo "[bucket] $(GCS_PREFIX) deleted"

# ──────────────────────────────────────────────────────────────────────────────
# Staging — stamp file, only re-uploads when src.zip or dataproc scripts change
# ──────────────────────────────────────────────────────────────────────────────
.staged: staging/src.zip dataproc/run_log_parsing.py dataproc/init-install.sh \
         benchmark/run_benchmark.py benchmark/plans.py .env.dataproc
	gcloud storage cp staging/src.zip              $(GCS_STAGING)/src.zip
	gcloud storage cp dataproc/run_log_parsing.py  $(GCS_STAGING)/run_log_parsing.py
	gcloud storage cp dataproc/init-install.sh     $(GCS_STAGING)/init-install.sh
	gcloud storage cp benchmark/run_benchmark.py   $(GCS_STAGING)/run_benchmark.py
	gcloud storage cp benchmark/plans.py           $(GCS_STAGING)/plans.py
	gcloud storage cp .env.dataproc                $(GCS_STAGING)/.env.dataproc
	touch .staged
	@echo "[stage] GCS staging artifacts are up to date"

# Compress the raw log — only re-runs if the source .log is newer than the .gz.
# gzip -c writes to stdout, leaving the original file untouched.
staging/access.log.gz: $(RAW_LOG_LOCAL)
	mkdir -p staging
	gzip -c $(RAW_LOG_LOCAL) > staging/access.log.gz
	@echo "[compress] staging/access.log.gz ready"

# Upload the compressed log to GCS.
# Depends on the compressed file so it only re-uploads when the local .gz was rebuilt.
# Parallel composite upload is disabled: it leaves unrecoverable partial chunks in GCS
# if the upload is interrupted, causing resume failures.
stage-raw-log: bucket-create staging/access.log.gz
	gcloud storage cp staging/access.log.gz $(GCS_RAW_LOG)
	@echo "[stage-raw-log] $(GCS_RAW_LOG) ready"

# ──────────────────────────────────────────────────────────────────────────────
# Cluster lifecycle
# ──────────────────────────────────────────────────────────────────────────────
cluster-create: .staged
	gcloud dataproc clusters create $(CLUSTER_NAME) \
	  --project=$(GCP_PROJECT) \
	  --region=$(REGION) \
	  --master-machine-type=n1-standard-2 \
	  --master-boot-disk-size=50GB \
	  --master-boot-disk-type=pd-ssd \
	  --worker-machine-type=n1-standard-4 \
	  --worker-boot-disk-size=100GB \
	  --worker-boot-disk-type=pd-ssd \
	  --num-workers=$(CLUSTER_WORKERS) \
	  --image-version=2.1-debian11 \
	  --initialization-actions=$(GCS_STAGING)/init-install.sh \
	  --properties=dataproc:dataproc.scheduler.max-concurrent-jobs=1 \
	  --enable-component-gateway
	@echo "[cluster] $(CLUSTER_NAME) created"

cluster-delete:
	gcloud dataproc clusters delete $(CLUSTER_NAME) \
	  --project=$(GCP_PROJECT) \
	  --region=$(REGION)
	@echo "[cluster] $(CLUSTER_NAME) deleted"

# ──────────────────────────────────────────────────────────────────────────────
# Jobs
# ──────────────────────────────────────────────────────────────────────────────

check-scale:
	@if [ -z "$(SCALE_PCT)" ]; then \
	  echo "[error] SCALE_PCT is required (allowed: $(SCALE_PCTS))"; \
	  exit 1; \
	fi
	@case " $(SCALE_PCTS) " in \
	  *" $(SCALE_PCT) "*) ;; \
	  *) echo "[error] SCALE_PCT=$(SCALE_PCT) is not in allowed set: $(SCALE_PCTS)"; exit 1 ;; \
	esac

# Ensures sampled Parquet for SCALE_PCT exists (Spark writes _SUCCESS on completion).
# Used by job-benchmark and job-plans.
check-parquet-ready: check-scale
	@if ! gcloud storage ls $(GCS_PARQUET_SCALE)/_SUCCESS >/dev/null 2>&1; then \
	  echo "[error] Parquet not ready for scale=$(SCALE_PCT): missing $(GCS_PARQUET_SCALE)/_SUCCESS"; \
	  echo "[error] Run: make job-log-parsing SCALE_PCT=$(SCALE_PCT)"; \
	  exit 1; \
	fi

# Prerequisite: parse raw access.log → Parquet in GCS.
# .env.dataproc (staged alongside src.zip) is loaded by run_log_parsing.py,
# so no --properties overrides are needed here.
# Skips submission when the parquet output already has a Spark _SUCCESS marker
# for the requested scale. Set FORCE=1 to re-run anyway.
job-log-parsing: check-scale .staged
	@if [ -z "$(FORCE)" ] && gcloud storage ls $(GCS_PARQUET_SCALE)/_SUCCESS >/dev/null 2>&1; then \
	  echo "[job] log-parsing already complete for scale=$(SCALE_PCT) at $(GCS_PARQUET_SCALE) (use FORCE=1 to re-run)"; \
	else \
	  gcloud dataproc jobs submit pyspark \
	    $(GCS_STAGING)/run_log_parsing.py \
	    --cluster=$(CLUSTER_NAME) \
	    --region=$(REGION) \
	    --project=$(GCP_PROJECT) \
	    --py-files=$(GCS_STAGING)/src.zip \
	    --files=$(GCS_STAGING)/.env.dataproc \
	    --properties=spark.sql.shuffle.partitions=$(SPARK_SHUFFLE_PARTITIONS) \
	    -- \
	    --sample-percent $(SCALE_PCT) \
	    --output-parquet-path $(GCS_PARQUET_SCALE) \
	  && echo "[job] log-parsing complete (scale=$(SCALE_PCT)) → $(GCS_PARQUET_SCALE)"; \
	fi

# Combined benchmark: wall-clock timing + stage metrics (shuffle/spill/tasks) in one Spark job.
job-benchmark: check-parquet-ready .staged
	gcloud dataproc jobs submit pyspark \
	  $(GCS_STAGING)/run_benchmark.py \
	  --cluster=$(CLUSTER_NAME) \
	  --region=$(REGION) \
	  --project=$(GCP_PROJECT) \
	  --py-files=$(GCS_STAGING)/src.zip \
	  --files=$(GCS_STAGING)/.env.dataproc \
	  --properties=spark.sql.shuffle.partitions=$(SPARK_SHUFFLE_PARTITIONS) \
	  -- \
	  --parquet-path $(GCS_PARQUET_SCALE) \
	  --wall-clock-output-path $(GCS_RESULTS_RUN)/allqueries_wall_clock.json \
	  --wall-clock-merged-output-path $(GCS_RESULTS_SCALE)/allqueries_wall_clock_merged.json \
	  --stage-metrics-output-path $(GCS_RESULTS_RUN)/stage_metrics.json \
	  --stage-metrics-merged-output-path $(GCS_RESULTS_SCALE)/stage_metrics_merged.json \
	  --wall-clock-num-runs $(WALL_CLOCK_NUM_RUNS) \
	  --stage-metrics-num-runs $(STAGE_METRICS_NUM_RUNS) \
	  --scale-pct $(SCALE_PCT) \
	  --benchmark-id $(BENCHMARK_ID)
	@echo "[job] benchmark complete (scale=$(SCALE_PCT), id=$(BENCHMARK_ID))"

# Plans benchmark: captures DataFrame query plans and RDD lineages for all queries.
job-plans: check-parquet-ready .staged
	gcloud dataproc jobs submit pyspark \
	  $(GCS_STAGING)/plans.py \
	  --cluster=$(CLUSTER_NAME) \
	  --region=$(REGION) \
	  --project=$(GCP_PROJECT) \
	  --py-files=$(GCS_STAGING)/src.zip \
	  --files=$(GCS_STAGING)/.env.dataproc \
	  --properties=spark.sql.shuffle.partitions=$(SPARK_SHUFFLE_PARTITIONS) \
	  -- \
	  --parquet-path $(GCS_PARQUET_SCALE) \
	  --output-dir $(GCS_RESULTS_RUN)/plans \
	  --scale-pct $(SCALE_PCT) \
	  --benchmark-id $(BENCHMARK_ID)
	@echo "[job] plans benchmark complete (scale=$(SCALE_PCT), id=$(BENCHMARK_ID))"

# ──────────────────────────────────────────────────────────────────────────────
# Fetch results back to local machine
# ──────────────────────────────────────────────────────────────────────────────
fetch-results: check-scale
	@if [ -n "$(FETCH_BENCHMARK_ID)" ]; then \
	  SRC="$(GCS_RESULTS_SCALE)/id=$(FETCH_BENCHMARK_ID)"; \
	  DST="$(LOCAL_RESULTS_SCALE)/id=$(FETCH_BENCHMARK_ID)"; \
	  mkdir -p "$$DST"; \
	  gcloud storage rsync -r "$$SRC" "$$DST" 2>/dev/null \
	    && echo "[fetch] fetched scale=$(SCALE_PCT) id=$(FETCH_BENCHMARK_ID)" \
	    || echo "[fetch] no artifacts for scale=$(SCALE_PCT) id=$(FETCH_BENCHMARK_ID), skipping"; \
	else \
	  mkdir -p "$(LOCAL_RESULTS_SCALE)"; \
	  gcloud storage rsync -r "$(GCS_RESULTS_SCALE)" "$(LOCAL_RESULTS_SCALE)" 2>/dev/null \
	    && echo "[fetch] fetched all IDs for scale=$(SCALE_PCT)" \
	    || echo "[fetch] no artifacts for scale=$(SCALE_PCT), skipping"; \
	fi
	python -m benchmark.merge_results --kind wall_clock --source-dir "$(LOCAL_RESULTS_SCALE)" --merged-output "$(LOCAL_RESULTS_SCALE)/allqueries_wall_clock_merged.json"
	python -m benchmark.merge_results --kind stage_metrics --source-dir "$(LOCAL_RESULTS_SCALE)" --merged-output "$(LOCAL_RESULTS_SCALE)/stage_metrics_merged.json"
	@echo "[fetch] merged rollups updated under $(LOCAL_RESULTS_SCALE)"

# ──────────────────────────────────────────────────────────────────────────────
# Help
# ──────────────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "Targets:"
	@echo "  setup-services         Enable required GCP APIs (once, before setup-iam)"
	@echo "  setup-iam              Grant Dataproc Worker role to default service account (once)"
	@echo "  bucket-create          Create GCS bucket if it does not already exist"
	@echo "  bucket-delete          Delete GCS bucket and ALL its contents (manual only)"
	@echo "  staging/access.log.gz  Compress RAW_LOG_LOCAL with gzip (only if source changed)"
	@echo "  stage-raw-log          Compress then upload access.log.gz to GCS"
	@echo "  staging/src.zip        Re-package src/ only when Python sources change"
	@echo "  .staged                Re-upload GCS staging artifacts only when files change"
	@echo "  cluster-create         Provision Dataproc cluster"
	@echo "  cluster-delete         Tear down Dataproc cluster"
	@echo "  job-log-parsing        Submit log-parsing preprocessing job (requires SCALE_PCT)"
	@echo "  check-parquet-ready    Fail if sampled Parquet for SCALE_PCT has no _SUCCESS (job-benchmark, job-plans)"
	@echo "  job-benchmark          Submit wall-clock + stage-metrics benchmark (requires SCALE_PCT; needs Parquet)"
	@echo "  job-plans              Submit plans benchmark (requires SCALE_PCT; needs Parquet)"
	@echo "  fetch-results          Fetch+merge results for SCALE_PCT (optional FETCH_BENCHMARK_ID)"
	@echo "                         Benchmark repeat counts: WALL_CLOCK_NUM_RUNS, STAGE_METRICS_NUM_RUNS"
	@echo "                         Allowed SCALE_PCTS: $(SCALE_PCTS)"
	@echo ""
	@echo "Prerequisites (one-time):"
	@echo "  1. Copy .env.dataproc.example → .env.dataproc and fill in your values"
	@echo "  2. make setup-services   — enable required GCP APIs"
	@echo "  3. make setup-iam        — grant the Dataproc Worker role"
	@echo "  4. make bucket-create    — create the GCS bucket"
	@echo "  5. make stage-raw-log    — compress and upload access.log to GCS"
	@echo ""
	@echo "Pipeline (run in order):"
	@echo "  make cluster-create → make job-log-parsing SCALE_PCT=5 → make job-benchmark SCALE_PCT=5 → make fetch-results SCALE_PCT=5 → make cluster-delete"
	@echo ""
