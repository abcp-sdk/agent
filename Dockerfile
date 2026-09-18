# syntax=docker/dockerfile:1
# Base images default to the in-cluster artifact registry (buildkitd trusts it
# as an insecure registry); override with --build-arg when building elsewhere.
# Note: forgejo mirrors the official library/* images under root/ (jrlab
# pull-through), so use root/ here.
ARG REGISTRY=docker.io
FROM ${REGISTRY}/root/node:26-alpine AS build
ARG HTTP_PROXY
ARG HTTPS_PROXY
ENV HTTP_PROXY=${HTTP_PROXY} \
    HTTPS_PROXY=${HTTPS_PROXY} \
    NO_PROXY=localhost,127.0.0.1,.svc.cluster.local,.svc
WORKDIR /build
# The sibling SDK repos (@abc-protocol/sdk, @abc-protocol/bundled-extension,
# @abcp/agent-sdk) are consumed via `file:../..` links, so copy them in first at
# the paths the .npmrc/package.json point at: they live one level above `agent`.
COPY agent/ .
COPY abc-protocol-typescript /abc-protocol-typescript
COPY bundled-extension /bundled-extension
COPY agent-sdk-typescript /agent-sdk-typescript
# No --ignore-scripts: the root package.json allowScripts whitelist gates
# which packages may run lifecycle scripts (esbuild builds this package's
# bundled TS). prepare builds the schema dist, which the server step needs.
RUN --mount=type=cache,target=/root/.npm \
    npm ci --no-audit --strict-ssl=false \
    && npm run build

# The binary embeds Node + all JS dependencies; the runtime stage needs only
# libc + CA certs.
FROM ${REGISTRY}/root/alpine:3.24
RUN sed -i 's|dl-cdn.alpinelinux.org|mirrors.aliyun.com|g' /etc/apk/repositories \
    && apk add --no-cache ca-certificates libstdc++
COPY --from=build /build/.sea/abcp-agent /usr/local/bin/abcp-agent
EXPOSE 8080
ENTRYPOINT ["abcp-agent"]
