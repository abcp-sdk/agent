{{/* Object-name prefix so a second infra release coexists in the same
     namespace with distinct names (`s2-nats`, ...). Empty = original names. */}}
{{- define "agent.prefix" -}}
{{- .Values.global.namePrefix | default "" -}}
{{- end -}}

{{/* StorageClass for every PVC. */}}
{{- define "agent.storageClass" -}}
{{- .Values.global.storageClass | default "workspace-local" -}}
{{- end -}}

{{/*
  proxyEnv - inject HTTP/HTTPS proxy environment variables
*/}}
{{- define "agent.proxyEnv" -}}
- name: http_proxy
  value: {{ .Values.global.proxy.http | quote }}
- name: https_proxy
  value: {{ .Values.global.proxy.https | quote }}
- name: HTTP_PROXY
  value: {{ .Values.global.proxy.http | quote }}
- name: HTTPS_PROXY
  value: {{ .Values.global.proxy.https | quote }}
- name: NO_PROXY
  value: {{ .Values.global.proxy.noProxy | quote }}
- name: no_proxy
  value: {{ .Values.global.proxy.noProxy | quote }}
{{- end }}
