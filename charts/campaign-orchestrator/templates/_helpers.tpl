{{- define "campaign-orchestrator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "campaign-orchestrator.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "campaign-orchestrator.labels" -}}
app.kubernetes.io/name: {{ include "campaign-orchestrator.name" . }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "campaign-orchestrator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "campaign-orchestrator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "campaign-orchestrator.fullnameWithSuffix" -}}
{{- $suffix := index . 0 -}}
{{- $context := index . 1 -}}
{{- printf "%s-%s" (include "campaign-orchestrator.fullname" $context) $suffix | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "campaign-orchestrator.mongo.fullname" -}}
{{- include "campaign-orchestrator.fullnameWithSuffix" (list "mongo" .) -}}
{{- end -}}

{{- define "campaign-orchestrator.mongo.serviceName" -}}
{{- include "campaign-orchestrator.mongo.fullname" . -}}
{{- end -}}

{{- define "campaign-orchestrator.postgres.fullname" -}}
{{- include "campaign-orchestrator.fullnameWithSuffix" (list "postgres" .) -}}
{{- end -}}

{{- define "campaign-orchestrator.postgres.serviceName" -}}
{{- include "campaign-orchestrator.postgres.fullname" . -}}
{{- end -}}

{{- define "campaign-orchestrator.validateCredentialRef" -}}
{{- $path := index . 0 -}}
{{- $cred := index . 1 -}}
{{- if $cred.isSecret -}}
{{- $secretName := trim (default "" $cred.secretName) -}}
{{- $secretKey := trim (default "" $cred.secretKey) -}}
{{- if or (eq $secretName "") (eq $secretKey "") -}}
{{- fail (printf "%s.isSecret=true requires both %s.secretName and %s.secretKey" $path $path $path) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "campaign-orchestrator.validations" -}}
{{- include "campaign-orchestrator.validateCredentialRef" (list "app.apiKey" .Values.app.apiKey) -}}
{{- include "campaign-orchestrator.validateCredentialRef" (list "broker.password" .Values.broker.password) -}}
{{- include "campaign-orchestrator.validateCredentialRef" (list "app.minio.password" .Values.app.minio.password) -}}
{{- include "campaign-orchestrator.validateCredentialRef" (list "campaignRepository.mongo.auth.rootPassword" .Values.campaignRepository.mongo.auth.rootPassword) -}}
{{- include "campaign-orchestrator.validateCredentialRef" (list "campaignRepository.mongo.connectionUri" .Values.campaignRepository.mongo.connectionUri) -}}
{{- include "campaign-orchestrator.validateCredentialRef" (list "campaignRepository.postgres.auth.password" .Values.campaignRepository.postgres.auth.password) -}}
{{- include "campaign-orchestrator.validateCredentialRef" (list "campaignRepository.postgres.dsn" .Values.campaignRepository.postgres.dsn) -}}

{{- if .Values.app.apiKey.isSecret -}}
{{- else -}}
{{- $apiKey := trim (default "" .Values.app.apiKey.hardcoded) -}}
{{- if eq $apiKey "" -}}
{{- fail "app.apiKey.hardcoded is required when app.apiKey.isSecret=false" -}}
{{- end -}}
{{- if lt (len $apiKey) 32 -}}
{{- fail "app.apiKey.hardcoded must be at least 32 characters to satisfy application validation" -}}
{{- end -}}
{{- if gt (len $apiKey) 255 -}}
{{- fail "app.apiKey.hardcoded must be at most 255 characters to satisfy application validation" -}}
{{- end -}}
{{- end -}}

{{- if .Values.broker.password.isSecret -}}
{{- else -}}
{{- $brokerPassword := trim (default "" .Values.broker.password.hardcoded) -}}
{{- if eq $brokerPassword "" -}}
{{- fail "broker.password.hardcoded is required when broker.password.isSecret=false" -}}
{{- end -}}
{{- end -}}

{{- if .Values.ingress.enabled -}}
{{- if and (eq (trim (default "" .Values.ingress.hostname)) "") (eq (len .Values.ingress.extraHosts) 0) (eq (len .Values.ingress.extraRules) 0) -}}
{{- fail "ingress.enabled=true requires ingress.hostname, ingress.extraHosts, or ingress.extraRules" -}}
{{- end -}}
{{- end -}}

{{- if not (has .Values.campaignRepository.backend (list "memory" "mongo" "postgres")) -}}
{{- fail "campaignRepository.backend must be one of: memory, mongo, postgres" -}}
{{- end -}}

{{- if and (gt .Values.replicaCount 1) (eq .Values.campaignRepository.backend "memory") -}}
{{- fail "replicaCount > 1 requires campaignRepository.backend=mongo|postgres; memory backend is single-pod only" -}}
{{- end -}}

{{- if eq .Values.campaignRepository.backend "mongo" -}}
{{- if .Values.campaignRepository.mongo.connectionUri.isSecret -}}
{{- else -}}
{{- $mongoUri := trim (default "" .Values.campaignRepository.mongo.connectionUri.hardcoded) -}}
{{- if and (eq $mongoUri "") .Values.campaignRepository.mongo.auth.rootPassword.isSecret -}}
{{- fail "campaignRepository.mongo.connectionUri must be provided (hardcoded or secret) when campaignRepository.mongo.auth.rootPassword.isSecret=true" -}}
{{- end -}}
{{- if eq $mongoUri "" -}}
{{- $rootUsername := default "" .Values.campaignRepository.mongo.auth.rootUsername -}}
{{- $rootPassword := default "" .Values.campaignRepository.mongo.auth.rootPassword.hardcoded -}}
{{- if or (regexMatch ".*[@:/%].*" $rootUsername) (regexMatch ".*[@:/%].*" $rootPassword) -}}
{{- fail "campaignRepository.mongo.connectionUri.hardcoded must be provided when campaignRepository.mongo.auth.rootUsername or campaignRepository.mongo.auth.rootPassword.hardcoded contains reserved URI characters such as @, :, /, or %" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- if eq .Values.campaignRepository.backend "postgres" -}}
{{- if .Values.campaignRepository.postgres.dsn.isSecret -}}
{{- else -}}
{{- $postgresDsn := trim (default "" .Values.campaignRepository.postgres.dsn.hardcoded) -}}
{{- if and (eq $postgresDsn "") .Values.campaignRepository.postgres.auth.password.isSecret -}}
{{- fail "campaignRepository.postgres.dsn must be provided (hardcoded or secret) when campaignRepository.postgres.auth.password.isSecret=true" -}}
{{- end -}}
{{- if eq $postgresDsn "" -}}
{{- $postgresUsername := default "" .Values.campaignRepository.postgres.auth.username -}}
{{- $postgresPassword := default "" .Values.campaignRepository.postgres.auth.password.hardcoded -}}
{{- if or (regexMatch ".*[@:/%].*" $postgresUsername) (regexMatch ".*[@:/%].*" $postgresPassword) -}}
{{- fail "campaignRepository.postgres.dsn.hardcoded must be provided when campaignRepository.postgres.auth.username or campaignRepository.postgres.auth.password.hardcoded contains reserved URI characters such as @, :, /, or %" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "campaign-orchestrator.mongo.connectionUri" -}}
{{- $hardcodedUri := trim (default "" .Values.campaignRepository.mongo.connectionUri.hardcoded) -}}
{{- if ne $hardcodedUri "" -}}
{{- $hardcodedUri -}}
{{- else -}}
{{- printf "mongodb://%s:%s@%s/?authSource=admin" .Values.campaignRepository.mongo.auth.rootUsername .Values.campaignRepository.mongo.auth.rootPassword.hardcoded (include "campaign-orchestrator.mongo.serviceName" .) -}}
{{- end -}}
{{- end -}}

{{- define "campaign-orchestrator.postgres.dsn" -}}
{{- $hardcodedDsn := trim (default "" .Values.campaignRepository.postgres.dsn.hardcoded) -}}
{{- if ne $hardcodedDsn "" -}}
{{- $hardcodedDsn -}}
{{- else -}}
{{- printf "postgresql://%s:%s@%s/%s" .Values.campaignRepository.postgres.auth.username .Values.campaignRepository.postgres.auth.password.hardcoded (include "campaign-orchestrator.postgres.serviceName" .) .Values.campaignRepository.postgres.auth.database -}}
{{- end -}}
{{- end -}}
