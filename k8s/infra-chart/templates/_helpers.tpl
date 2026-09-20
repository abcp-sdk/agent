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
