# DEVELOP

Developer notes for `abcp-agent`. User/deploy docs live in `README.md`; the
deployment charts live in `abc-protocol/deploy`.

## Packages via artifact (not GitHub / npmjs)

The platform's private package registry is **artifact**
(`http://artifact.worker.svc.cluster.local`). **All internal packages are
published to and consumed from artifact.** Authoritative guide:
`easy-vcs/deploy:PUBLISHING.md`. This repo is both a **consumer**
(`@abc-protocol/sdk`, `@abc-protocol/bundled-extension`) and a **producer**
(`@abcp-agent/*`).

- **Consume** — `.npmrc`:
  ```
  registry=http://artifact.worker.svc.cluster.local/artifacts/npm/
  ```
  (the same mount serves upstream-cached packages **and** our `@scope` hosted
  ones). Read is **anonymous**; install needs no token.
- **Publish** — a **scoped** name (`@org/name`) + a write token:
  ```
  //artifact.worker.svc.cluster.local/artifacts/npm/:_authToken=<token>
  npm publish --registry=http://artifact.worker.svc.cluster.local/artifacts/npm/ --access public
  ```
  (dev write token: `dev-artifact-token`; real deployments set their own via the
  chart's `secrets.artifactToken`.)
- **Interim (a package not yet on artifact)** — depend on the **cluster Forgejo**
  git source, NOT `github:abcp-sdk/...` (npm fetches that from github.com, which
  lags/lacks the cluster tags):
  ```
  "@scope/pkg": "git+http://git.agent.svc.cluster.local/abc-protocol/<repo>.git#<tag>"
  ```
  then **regenerate `package-lock.json`** (otherwise `npm ci` keeps the old
  `resolved`).

### Status of our packages on artifact

Published (see `abc-protocol/agent` MR #6 / the SDK repos):

| Package | Version | Source |
|---|---|---|
| `@abc-protocol/sdk` | 3.10.0 | `abc-protocol/abc-protocol-typescript@v3.10.0` |
| `@abcp/agent-sdk` | 0.27.0 | `abc-protocol/agent-sdk-typescript@main` |
| `@abc-protocol/bundled-extension` | 0.14.0 | `abc-protocol/bundled-extension@main` |
| `agent_sdk` (pub) | 0.1.0 | `abc-protocol/agent-sdk-dart@main` |
| `com.abcp:agent-sdk-kotlin` (maven) | 0.1.0 | `abc-protocol/agent-sdk-kotlin@main` |

`@abc-protocol/sdk` is currently consumed via the **Forgejo git dep** (see
above); switching it to the artifact npm dep (`@abc-protocol/sdk@3.10.0`, which
is now published) is the intended follow-up.

## Build / verify

```sh
npm install        # or: npm ci  (uses package-lock.json)
npm run build      # schema → agent → server → .sea/abcp-agent (Node SEA)
npm run typecheck  # tsc -p packages/{agent,server}
npm test
```

## Building the image

`build-image.sh` builds the SEA binary in the cluster buildkitd and pushes to
the in-cluster registry (`git.agent.svc.cluster.local/abcp/agent:<tag>`). The
Dockerfile's `REGISTRY` build-arg defaults to `docker.io`; the library base
images live under `root/` on the **in-cluster** registry, so a cluster build
must pass `REGISTRY=git.agent.svc.cluster.local` (the script already does).
