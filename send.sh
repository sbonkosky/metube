#!/usr/bin/env bash
set -euo pipefail

docker build -t registry.bonk.website/metube:latest .
docker push registry.bonk.website/metube:latest

cd "$HOME/Projects/_homelab/docker/projects/media"
if command -v rdc >/dev/null 2>&1; then
  rdc pull metube
  rdc up -d --no-deps metube
else
  docker --context docker-vm compose pull metube
  docker --context docker-vm compose up -d --no-deps metube
fi
