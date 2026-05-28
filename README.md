# Helm Charts

This GitHub Pages site hosts Helm charts published from the
[campaign-orchestrator repository](https://github.com/INTERSECT-SDK/campaign-orchestrator).

## Usage

[Helm](https://helm.sh/) must be installed first.

Add the chart repository:

```bash
helm repo add intersect-campaign-orchestrator https://intersect-sdk.github.io/campaign-orchestrator
```

If you already added it before, refresh chart metadata:

```bash
helm repo update
```

List available charts:

```bash
helm search repo intersect-campaign-orchestrator
```

## Available Charts

- `campaign-orchestrator`

## Install

```bash
helm install campaign-orchestrator intersect-campaign-orchestrator/campaign-orchestrator \
  --namespace intersect \
  --create-namespace \
  --set app.apiKey.hardcoded=<min-32-char-api-key> \
  --set broker.password.hardcoded=<broker-password>
```

## Upgrade

```bash
helm upgrade campaign-orchestrator intersect-campaign-orchestrator/campaign-orchestrator \
  --namespace intersect \
  --set app.apiKey.hardcoded=<min-32-char-api-key> \
  --set broker.password.hardcoded=<broker-password>
```

## Uninstall

```bash
helm uninstall campaign-orchestrator --namespace intersect
```
