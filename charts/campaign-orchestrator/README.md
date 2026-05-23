# Campaign Orchestrator Helm Chart

This chart deploys the INTERSECT campaign orchestrator and can optionally co-locate
its campaign repository backend in the same release.

## Components

- Orchestrator Deployment
- Orchestrator Service
- Optional MongoDB StatefulSet and Service when `campaignRepository.backend=mongo`
- Optional PostgreSQL StatefulSet and Service when `campaignRepository.backend=postgres`

## Install

```bash
helm install campaign-orchestrator ./charts/campaign-orchestrator -n intersect --create-namespace
```

The default values run the orchestrator with the in-memory campaign repository.
That means only the application pod is created.

## Backend Options

### In-memory

```bash
helm upgrade --install campaign-orchestrator ./charts/campaign-orchestrator \
  -n intersect --create-namespace \
  --set campaignRepository.backend=memory
```

### MongoDB

```bash
helm upgrade --install campaign-orchestrator ./charts/campaign-orchestrator \
  -n intersect --create-namespace \
  --set campaignRepository.backend=mongo
```

This enables the MongoDB StatefulSet in the same release and points the
application at the in-cluster MongoDB service.

### PostgreSQL

```bash
helm upgrade --install campaign-orchestrator ./charts/campaign-orchestrator \
  -n intersect --create-namespace \
  --set campaignRepository.backend=postgres
```

This enables the PostgreSQL StatefulSet in the same release and points the
application at the in-cluster PostgreSQL service.

## Defaults

The chart mirrors the repository's local Docker Compose defaults for the broker
and repository credentials so it works as a demo install without extra values.
Override `app.apiKey` and the broker/database settings before using it in a real
environment.

## Validation

```bash
helm lint ./charts/campaign-orchestrator
helm template campaign-orchestrator ./charts/campaign-orchestrator
```
