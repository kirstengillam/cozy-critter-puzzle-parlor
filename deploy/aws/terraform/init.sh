#!/usr/bin/env bash
# Initializes this Terraform config against its S3 state backend. The
# bucket name embeds the AWS account id, which we don't want committed to
# git — so main.tf declares an empty `backend "s3" {}` and the real values
# are supplied here via -backend-config instead. Any extra arguments (e.g.
# -migrate-state, -reconfigure) are passed straight through to `terraform
# init`.
set -euo pipefail
: "${TF_STATE_BUCKET:?set TF_STATE_BUCKET first (the bucket from bootstrapping the backend)}"
cd "$(dirname "$0")"

terraform init \
  -backend-config="bucket=${TF_STATE_BUCKET}" \
  -backend-config="key=cozy-critter/terraform.tfstate" \
  -backend-config="region=us-east-1" \
  -backend-config="use_lockfile=true" \
  "$@"
