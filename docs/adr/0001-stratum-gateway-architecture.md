# ADR 0001: Stratum Gateway Architecture

- Status: Accepted
- Date: 2026-02-16
- Deciders: AZCoin engineering

## Context

AZCoin needs a reliable path for ingesting miner Stratum traffic at the network edge and forwarding it to a central processing layer. We want to move quickly with a service that is easy to containerize, easy to deploy, and explicit about security hygiene.

Early development must avoid overcommitting to protocol details before baseline runtime and operational conventions are in place. The initial milestone is a running gateway service process with predictable startup, network binding, and health logging behavior.

## Decision

For gateway V1 and V2 architecture, we adopt the following:

1. **Topology**: V1 edge nodes receive external miner traffic and forward toward V2 core services.
2. **Implementation language**: Gateway services are implemented in Rust.
3. **Packaging and runtime model**: Docker-first workflow for build, test, and deployment.
4. **Repository security baseline**: No secrets are stored in the repository. Configuration that may contain sensitive values is injected via environment-specific mechanisms outside git.

## Rationale

- **Edge-to-core split** supports operational isolation and phased scaling.
- **Rust** offers strong performance, memory safety, and predictable concurrency characteristics for long-running network services.
- **Docker-first** simplifies consistent execution across developer machines, CI, and deployment environments.
- **No secrets in repo** reduces accidental credential exposure risk and aligns with secure software delivery practices.

## Consequences

### Positive

- Clear initial architecture boundary: edge ingress and core processing can evolve semi-independently.
- Early foundation aligns with production deployment requirements.
- Security posture is explicit from day one.

### Trade-offs

- Additional container and orchestration overhead during local development.
- Rust onboarding may require ramp-up for engineers less familiar with the ecosystem.
- V1 scaffold intentionally postpones protocol completeness, so near-term functionality is limited while architecture is established.

## Follow-up

- Define V1-to-V2 transport contract and message framing.
- Add container artifacts (`Dockerfile`, optional compose/dev scripts).
- Introduce observability standards (structured logs, metrics, readiness/liveness strategy).
