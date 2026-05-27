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
  --set app.apiKey.hardcoded=<your-api-key> \
  --set broker.password.hardcoded=<your-broker-password>
```

The default values run the orchestrator with the in-memory campaign repository.
That means only the application pod is created.

The chart requires an API key source at render/install time. If no API key
source is configured, template rendering fails.

## Credential Pattern

Sensitive values use the same structure as broker-http-proxy charts:

```yaml
<field>:
  isSecret: false
  hardcoded: ""
  secretName: ""
  secretKey: ""
```

- `isSecret: false` uses `hardcoded`
- `isSecret: true` reads from `secretName` + `secretKey`

## API Key Configuration

Option 1: Set hardcoded key (recommended for simple installs)

The application requires a minimum API key length of 32 characters.

```bash
helm upgrade --install campaign-orchestrator ./charts/campaign-orchestrator \
  -n intersect --create-namespace \
  --set app.apiKey.hardcoded=<your-api-key> \
  --set broker.password.hardcoded=<your-broker-password>
```

Option 2: Reference an existing secret (advanced/umbrella use)

```bash
helm upgrade --install campaign-orchestrator ./charts/campaign-orchestrator \
  -n intersect --create-namespace \
  --set app.apiKey.isSecret=true \
  --set app.apiKey.secretName=<secret-name> \
  --set app.apiKey.secretKey=<secret-key>
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
For production, prefer `isSecret: true` for all sensitive credential fields.

## Validation

```bash
helm lint ./charts/campaign-orchestrator \
  --set app.apiKey.hardcoded=<min-32-char-key> \
  --set broker.password.hardcoded=<broker-password>
helm template campaign-orchestrator ./charts/campaign-orchestrator \
  --set app.apiKey.hardcoded=<min-32-char-key> \
  --set broker.password.hardcoded=<broker-password>
```
