# syntax=docker/dockerfile:1
# Base images default to the in-cluster artifact registry (buildkitd trusts it
# as an insecure registry); override with --build-arg when building elsewhere.
ARG REGISTRY=docker.io
FROM ${REGISTRY}/library/node:26-alpine AS build
ARG HTTP_PROXY
ARG HTTPS_PROXY
ENV HTTP_PROXY=${HTTP_PROXY} \
    HTTPS_PROXY=${HTTPS_PROXY} \
    NO_PROXY=localhost,127.0.0.1,.svc.cluster.local,.svc
WORKDIR /build
COPY package.json package-lock.json tsconfig.base.json .npmrc ./
COPY scripts scripts
COPY packages packages
RUN --mount=type=cache,target=/root/.npm \
    npm ci --ignore-scripts --no-audit --strict-ssl=false \
    && npm rebuild esbuild \
    && npm run build

# The binary embeds Node, all JS dependencies and the SPA; the runtime stage
# needs only libc + CA certs.
FROM ${REGISTRY}/library/alpine:3.24
RUN sed -i 's|dl-cdn.alpinelinux.org|mirrors.aliyun.com|g' /etc/apk/repositories \
    && apk add --no-cache ca-certificates libstdc++
COPY --from=build /build/.sea/easylab-agent /usr/local/bin/easylab-agent
ENV ZERGX_PORT=8080
EXPOSE 8080
ENTRYPOINT ["easylab-agent"]
