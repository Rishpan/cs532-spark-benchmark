#!/bin/bash
# Dataproc initialization action — runs on all nodes at cluster startup.
# Installs Python dependencies not pre-bundled in the Dataproc image.
set -euxo pipefail

pip install python-dotenv
