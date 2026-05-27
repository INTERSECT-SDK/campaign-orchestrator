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
helm install campaign-orchestrator ./charts/campaign-orchestrator -n intersect --create-namespace \
  --set app.apiKey=<your-api-key>
```

The default values run the orchestrator with the in-memory campaign repository.
That means only the application pod is created.

The chart requires an API key at render/install time. If no API key source is
configured, template rendering fails.

## API Key Configuration

Option 1: Set inline key value (recommended for simple installs)

The application requires a minimum API key length of 32 characters.

```bash
helm upgrade --install campaign-orchestrator ./charts/campaign-orchestrator \
  -n intersect --create-namespace \
  --set app.apiKey=<your-api-key>
```

Option 2: Reference an existing secret (advanced/umbrella use)

```bash
helm upgrade --install campaign-orchestrator ./charts/campaign-orchestrator \
  -n intersect --create-namespace \
  --set app.apiKeyExistingSecret.enabled=true \
  --set app.apiKeyExistingSecret.name=<secret-name> \
  --set app.apiKeyExistingSecret.key=<secret-key>
```

You can also start from this example values file:

```bash
helm upgrade --install campaign-orchestrator ./charts/campaign-orchestrator \
  -n intersect --create-namespace \
  -f ./charts/campaign-orchestrator/examples/values-umbrella-existing-secret.yaml
```

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
and repository credentials so it works as a demo install with minimal overrides.
Set API key configuration and broker/database settings appropriate for your
environment.

## Validation

```bash
helm lint ./charts/campaign-orchestrator --set app.apiKey=<min-32-char-key>
helm template campaign-orchestrator ./charts/campaign-orchestrator --set app.apiKey=<min-32-char-key>
```
