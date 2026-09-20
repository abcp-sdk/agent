{{/* Namespace: release namespace unless overridden. */}}
{{- define "abcp-agent.namespace" -}}
{{- .Values.namespaceOverride | default .Release.Namespace -}}
{{- end -}}

{{- define "abcp-agent.labels" -}}
app.kubernetes.io/name: abcp-agent
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "abcp-agent.saName" -}}
{{- if .Values.serviceAccount.create -}}
{{- .Values.serviceAccount.name | default "abcp-agent" -}}
{{- else -}}
default
{{- end -}}
{{- end -}}

{{/* In-cluster NATS URL for the per-application `agent` account (infra). */}}
{{- define "abcp-agent.natsUrl" -}}
{{- printf "nats://%s:%s@%s:%v" .Values.infra.nats.user .Values.infra.nats.password .Values.infra.nats.host .Values.infra.nats.port -}}
{{- end -}}

{{/* In-cluster easyworker URL the worker extension drives (h1 Connect RPC). */}}
{{- define "abcp-agent.workerUrl" -}}
{{- printf "http://%s.%s.svc.cluster.local:80" .Values.worker.service.name (include "abcp-agent.namespace" .) -}}
{{- end -}}

{{/* S3 object-store env (durable file bytes leave NATS). */}}
{{- define "abcp-agent.objectStoreEnv" -}}
- name: AGENT_BLOB_BACKEND
  value: "s3"
- name: S3_BUCKET
  value: {{ .Values.infra.s3.bucket | quote }}
- name: S3_REGION
  value: {{ .Values.infra.s3.region | quote }}
- name: S3_ENDPOINT
  value: {{ .Values.infra.s3.endpoint | quote }}
- name: S3_ACCESS_KEY
  value: {{ .Values.infra.s3.accessKey | quote }}
- name: S3_SECRET_KEY
  value: {{ .Values.infra.s3.secretKey | quote }}
- name: S3_PATH_STYLE
  value: {{ .Values.infra.s3.pathStyle | toString | quote }}
- name: S3_PREFIX
  value: {{ .Values.infra.s3.prefix | quote }}
{{- end -}}
