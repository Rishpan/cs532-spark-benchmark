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

.PHONY: run-all setup-services setup-iam bucket-create bucket-delete cluster-create cluster-delete \
        job-log-parsing job-benchmark \
        stage-raw-log fetch-results help


# ──────────────────────────────────────────────────────────────────────────────
# run-all: full end-to-end pipeline
# ──────────────────────────────────────────────────────────────────────────────
run-all: .staged cluster-create job-log-parsing job-benchmark cluster-delete fetch-results

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
.staged: bucket-create staging/src.zip dataproc/run_log_parsing.py dataproc/init-install.sh benchmark/wall_clock.py .env.dataproc
	gcloud storage cp staging/src.zip             $(GCS_STAGING)/src.zip
	gcloud storage cp dataproc/run_log_parsing.py $(GCS_STAGING)/run_log_parsing.py
	gcloud storage cp dataproc/init-install.sh    $(GCS_STAGING)/init-install.sh
	gcloud storage cp benchmark/wall_clock.py     $(GCS_STAGING)/wall_clock.py
	gcloud storage cp .env.dataproc               $(GCS_STAGING)/.env.dataproc
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
cluster-create:
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
	  --enable-component-gateway
	@echo "[cluster] $(CLUSTER_NAME) created"

cluster-delete:
	gcloud dataproc clusters delete $(CLUSTER_NAME) \
	  --project=$(GCP_PROJECT) \
	  --region=$(REGION) \
	@echo "[cluster] $(CLUSTER_NAME) deleted"

# ──────────────────────────────────────────────────────────────────────────────
# Jobs
# ──────────────────────────────────────────────────────────────────────────────

# Prerequisite: parse raw access.log → Parquet in GCS.
# .env.dataproc (staged alongside src.zip) is loaded by run_log_parsing.py,
# so no --properties overrides are needed here.
job-log-parsing: .staged
	gcloud dataproc jobs submit pyspark \
	  $(GCS_STAGING)/run_log_parsing.py \
	  --cluster=$(CLUSTER_NAME) \
	  --region=$(REGION) \
	  --project=$(GCP_PROJECT) \
	  --py-files=$(GCS_STAGING)/src.zip \
	  --files=$(GCS_STAGING)/.env.dataproc \
	  --properties=spark.sql.shuffle.partitions=$(SPARK_SHUFFLE_PARTITIONS)
	@echo "[job] log-parsing complete → $(GCS_PARQUET)"

# Wall-clock benchmark: runs RDD / DataFrame / SQL variants of error_pattern_analysis.
job-benchmark: .staged
	gcloud dataproc jobs submit pyspark \
	  $(GCS_STAGING)/wall_clock.py \
	  --cluster=$(CLUSTER_NAME) \
	  --region=$(REGION) \
	  --project=$(GCP_PROJECT) \
	  --py-files=$(GCS_STAGING)/src.zip \
	  --files=$(GCS_STAGING)/.env.dataproc \
	  --properties=spark.sql.shuffle.partitions=$(SPARK_SHUFFLE_PARTITIONS) \
	  -- \
	  --parquet-path $(GCS_PARQUET) \
	  --output-path $(RESULTS_PATH)
	@echo "[job] benchmark complete → $(RESULTS_PATH)"

# ──────────────────────────────────────────────────────────────────────────────
# Fetch results back to local machine
# ──────────────────────────────────────────────────────────────────────────────
fetch-results:
	mkdir -p results
	gcloud storage cp $(RESULTS_PATH) results/error_pattern_wall_clock.json
	@echo "[fetch] results saved to results/error_pattern_wall_clock.json"

# ──────────────────────────────────────────────────────────────────────────────
# Help
# ──────────────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "Targets:"
	@echo "  run-all                Full pipeline: stage → cluster → jobs → delete → fetch"
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
	@echo "  job-log-parsing        Submit log-parsing preprocessing job"
	@echo "  job-benchmark          Submit wall-clock benchmark job (error_pattern_analysis)"
	@echo "  fetch-results          Copy benchmark JSON results back to results/"
	@echo ""
	@echo "Prerequisites:"
	@echo "  1. Copy .env.dataproc.example → .env.dataproc and fill in your values"
	@echo "  2. Run 'make setup-services' once to enable required GCP APIs"
	@echo "  3. Run 'make setup-iam' once to grant the Dataproc Worker role"
	@echo "  4. Run 'make stage-raw-log' once to compress and upload the raw access.log"
	@echo "  5. Run 'make run-all' (or individual targets) to execute the pipeline"
	@echo ""
