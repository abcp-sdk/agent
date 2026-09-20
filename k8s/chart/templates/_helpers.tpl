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

{{/* In-cluster NATS URL (the standalone broker Deployment). */}}
{{- define "abcp-agent.natsUrl" -}}
{{- printf "nats://%s.%s.svc.cluster.local:4222" .Values.nats.service.name (include "abcp-agent.namespace" .) -}}
{{- end -}}

{{/* In-cluster easyworker URL the workspace extension drives (h1 Connect RPC). */}}
{{- define "abcp-agent.easyworkerUrl" -}}
{{- if .Values.workspace.workerUrl -}}
{{- .Values.workspace.workerUrl -}}
{{- else -}}
{{- printf "http://%s.%s.svc.cluster.local:80" .Values.easyworker.service.name (include "abcp-agent.namespace" .) -}}
{{- end -}}
{{- end -}}

{{/* S3 object-store env (durable file bytes leave NATS). */}}
{{- define "abcp-agent.objectStoreEnv" -}}
{{- if .Values.objectStore.enabled }}
- name: AGENT_BLOB_BACKEND
  value: "s3"
- name: S3_BUCKET
  value: {{ .Values.objectStore.bucket | quote }}
- name: S3_REGION
  value: {{ .Values.objectStore.region | quote }}
- name: S3_ENDPOINT
  value: {{ .Values.objectStore.endpoint | quote }}
- name: S3_ACCESS_KEY
  value: {{ .Values.objectStore.accessKey | quote }}
- name: S3_SECRET_KEY
  value: {{ .Values.objectStore.secretKey | quote }}
- name: S3_PATH_STYLE
  value: {{ .Values.objectStore.pathStyle | toString | quote }}
- name: S3_PREFIX
  value: {{ .Values.objectStore.prefix | quote }}
{{- end }}
{{- end -}}
