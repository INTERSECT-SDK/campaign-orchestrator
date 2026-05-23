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
