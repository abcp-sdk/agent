#!/usr/bin/env bash
# Build and push the abcp-agent image WITHOUT a local build daemon.
# Mirrors upstream build-image.sh: buildkitd (in-cluster) -> docker archive ->
# skopeo -> forgejo OCI.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Base-image registry (the Dockerfile's `${REGISTRY}/root/<img>` bases). Kept on
# the in-cluster registry that buildkitd trusts as INSECURE; override if your
# buildkitd also trusts the artifact registry.
REGISTRY="${REGISTRY:-git.agent.svc.cluster.local}"
# Push destination. The platform pulls `abc-protocol/agent` from the artifact
# registry, so images are published there (plain HTTP in-cluster ->
# --dest-tls-verify=false).
DEST_REGISTRY="${DEST_REGISTRY:-artifact.worker.svc.cluster.local}"
NAMESPACE="${NAMESPACE:-abc-protocol}"
NAME="${NAME:-agent}"
TAG="${TAG:-$(date +%Y%m%d%H%M%S)}"
DEST="${DEST_REGISTRY}/${NAMESPACE}/${NAME}:${TAG}"
BUILDKIT="${BUILDKIT_ADDR:-tcp://buildkitd.temp.svc.cluster.local:1234}"
# Write credential for the artifact registry (anonymous PULL; publish needs a
# write token). `root:$ARTIFACT_TOKEN`.
ARTIFACT_USER="${ARTIFACT_USER:-root}"
ARTIFACT_TOKEN="${ARTIFACT_TOKEN:-dev-artifact-token}"
PROXY="${PROXY:-http://mihomo.develop.svc.cluster.local:7890}"
# Containerfile lives in ./; the context is the repo root so COPY path match.
DOCKERFILE="Dockerfile"

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

echo "Building agent image -> ${DEST} (buildkitd=${BUILDKIT})"
buildctl --addr "${BUILDKIT}" build \
  --frontend dockerfile.v0 \
  --local "context=${DIR}" \
  --local "dockerfile=${DIR}" \
  --opt "filename=${DOCKERFILE}" \
  --opt "build-arg:REGISTRY=${REGISTRY}" \
  --opt "build-arg:HTTP_PROXY=${PROXY}" \
  --opt "build-arg:HTTPS_PROXY=${PROXY}" \
  --opt "build-arg:NO_PROXY=localhost,127.0.0.1,.svc.cluster.local,.svc,.nip.io,10.199.64.20,develop.10.199.64.20.nip.io" \
  --output "type=docker,name=${NAMESPACE}/${NAME}:${TAG},dest=${WORK}/image.tar" \
  --progress plain

echo "Pushing to artifact ${DEST}"
skopeo copy \
  --dest-creds "${ARTIFACT_USER}:${ARTIFACT_TOKEN}" \
  --dest-tls-verify=false \
  "docker-archive:${WORK}/image.tar:${NAMESPACE}/${NAME}:${TAG}" \
  "docker://${DEST}"

echo "Verifying push:"
skopeo inspect --creds "${ARTIFACT_USER}:${ARTIFACT_TOKEN}" --tls-verify=false "docker://${DEST}" >/dev/null 2>&1 \
  && echo "OK ${DEST}" \
  || echo "inspect failed for ${DEST} (image may still be present)"
