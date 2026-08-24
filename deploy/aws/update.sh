#!/usr/bin/env bash
# Run on the EC2 box (e.g. via `aws ssm start-session`) to deploy the latest
# main branch. Requires the repo already cloned to /opt/cozy-critter with the
# GitHub deploy key set up (see deploy/aws/terraform/README or the plan doc).
set -euo pipefail

cd /opt/cozy-critter
git pull
docker build -t cozy-critter-gateway:latest -f Dockerfile .
docker compose -f deploy/aws/docker-compose.yml up -d
