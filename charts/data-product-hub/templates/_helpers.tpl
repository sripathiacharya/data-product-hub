{{- define "data-product-hub.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "data-product-hub.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "data-product-hub.name" . -}}
{{- if .Release.Name -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end }}

{{- define "data-product-hub.engine.fullname" -}}
{{ include "data-product-hub.fullname" . }}-engine
{{- end }}

{{- define "data-product-hub.operator.fullname" -}}
{{ include "data-product-hub.fullname" . }}-operator
{{- end }}
