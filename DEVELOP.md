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

**Published to artifact** (every language; verified resolvable from the mount):

| Ecosystem | Package | Version | Source repo |
|---|---|---|---|
| npm | `@abc-protocol/sdk` | 3.10.0 | `abc-protocol/abc-protocol-typescript@v3.10.0` |
| npm | `@abcp/agent-sdk` | 0.27.0 | `abc-protocol/agent-sdk-typescript@main` |
| npm | `@abc-protocol/bundled-extension` | 0.14.0 | `abc-protocol/bundled-extension@main` |
| pub (Dart) | `agent_sdk` | 0.1.0 | `abc-protocol/agent-sdk-dart@main` |
| Maven (Kotlin) | `com.abcp:agent-sdk-kotlin` | 0.1.0 | `abc-protocol/agent-sdk-kotlin@main` |
| Swift registry | `abc-protocol.agent-sdk-swift` | 0.1.0 | `abc-protocol/agent-sdk-swift@v0.1.0` |
| Go proxy | `github.com/abcp-sdk/abc-protocol-go/v2` | v2.7.0 | `abc-protocol/abc-protocol-go@v2.7.0` |
| Go proxy | `github.com/abcp-sdk/agent-sdk-go` | v0.27.0 | `abc-protocol/agent-sdk-go@main` |

### Publishing recipes (per ecosystem, artifact `worker` mount)

`ARTIFACT=http://artifact.worker.svc.cluster.local`; write token
`dev-artifact-token` (read is anonymous).

| Ecosystem | Publish |
|---|---|
| **npm** | `.npmrc`: `registry=$ARTIFACT/artifacts/npm/` + `//artifact.../artifacts/npm/:_authToken=<token>`; `npm publish --access public` (scoped) |
| **pub (Dart)** | a `pub-tokens.json` at `~/.config/dart/pub-tokens.json` = `{"version":1,"hosted":[{"url":"$ARTIFACT/artifacts/pub/","token":"<token>"}]}`; `PUB_HOSTED_URL=$ARTIFACT/artifacts/pub dart pub publish --force` (needs `LICENSE`, `CHANGELOG.md`, no `publish_to: none`) |
| **Maven/Gradle** | `publishing { repositories { maven { url="$ARTIFACT/artifacts/maven/"; isAllowInsecureProtocol=true; credentials{ username="root"; password=<token> } } } }`; `gradle publish` |
| **Swift registry** | `swift package-registry publish` refuses plain HTTP; PUT the source-archive zip directly: `curl -X PUT --data-binary @pkg.zip -H "authorization: Bearer <token>" $ARTIFACT/artifacts/swift/<scope>/<name>/<version>` |
| **Go proxy** | zip rooted at `<module>@<version>/`; `curl -X PUT --data-binary @module.zip -H "authorization: Bearer <token>" "$ARTIFACT/artifacts/go/upload?name=<module>&version=vX.Y.Z"` |

`@abc-protocol/sdk` is currently still consumed via the **Forgejo git dep**; the
published npm package (`@abc-protocol/sdk@3.10.0`) is the intended follow-up
(switch `package.json` to `^3.10.0` + `.npmrc` registry → artifact, and
regenerate the lockfile).

## Build / verify

```sh
npm install        # or: npm ci  (uses package-lock.json)
npm run build      # schema → agent → server → .sea/abcp-agent (Node SEA)
npm run typecheck  # tsc -p packages/{agent,server}
npm test
```

## Building the image

`build-image.sh` builds the SEA binary in the cluster buildkitd and pushes to
the **artifact** registry (`artifact.worker.svc.cluster.local/abc-protocol/agent:<tag>`),
which is what the platform pulls from.

- **Base images** come from `${REGISTRY}/root/<img>` (buildkitd trusts
  `git.agent.svc.cluster.local` as an insecure registry; artifact also serves
  `root/node:26-alpine` / `root/alpine:3.24` if your buildkitd trusts it).
- **Push destination** is `DEST_REGISTRY` (default
  `artifact.worker.svc.cluster.local`), pushed with
  `skopeo copy --dest-creds root:$ARTIFACT_TOKEN --dest-tls-verify=false`.
- The **Dockerfile frontend** is pulled from a China mirror
  (`# syntax=docker.m.daocloud.io/docker/dockerfile:1`) because docker.io egress
  is flaky in-cluster.
