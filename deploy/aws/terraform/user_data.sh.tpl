#!/usr/bin/env bash
set -euo pipefail

dnf install -y docker git
systemctl enable --now docker

mkdir -p /usr/libexec/docker/cli-plugins
curl -sSL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" \
  -o /usr/libexec/docker/cli-plugins/docker-compose
chmod +x /usr/libexec/docker/cli-plugins/docker-compose

mkdir -p /opt/cozy-critter

# Repo clone is intentionally NOT automated here: this is a private repo, and
# the GitHub deploy key is pasted in manually over SSM after `terraform apply`
# rather than stored in Parameter Store, to avoid granting this instance's
# IAM role decrypt access to a credential it only needs once. See
# deploy/aws/terraform (plan doc) for the exact clone + `docker compose up`
# steps to run by hand after connecting with `aws ssm start-session`.
