{{- define "bobs.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Resolve and validate the ingress controller flavour for the bobs subchart.
The parent chart's `global.ingress.controller` propagates to subcharts via
Helm's standard `global:` mechanism. Standalone bobs renders fall back to
the subchart's own `global.ingress.controller` default.
*/}}
{{- define "bobs.ingressController" -}}
{{- $c := (((.Values.global).ingress).controller) | default "nginx-inc" -}}
{{- $supported := list "nginx-inc" "nginx-community" -}}
{{- if not (has $c $supported) -}}
{{- fail (printf "global.ingress.controller=%q is not supported in bobs subchart. Supported values: %v" $c $supported) -}}
{{- end -}}
{{- $c -}}
{{- end -}}

{{- define "bobs.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "bobs.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "bobs.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "bobs.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "bobs.selectorLabels" -}}
app.kubernetes.io/name: {{ include "bobs.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Build the BOBS image reference. global.imageRegistry overrides image.registry.
A qualified repository is split before applying the global override, avoiding
references such as registry.example/eccr.example/project/bobs.
*/}}
{{- define "bobs.image" -}}
{{- $globalRegistry := "" -}}
{{- if .Values.global -}}
  {{- $globalRegistry = .Values.global.imageRegistry | default "" | trimSuffix "/" -}}
{{- end -}}
{{- $imageRegistry := .Values.image.registry | default "" | trimSuffix "/" -}}
{{- $repository := required "image.repository must be set" .Values.image.repository | trimPrefix "/" -}}
{{- $parts := splitList "/" $repository -}}
{{- $repositoryRegistry := "" -}}
{{- $repositoryPath := $repository -}}
{{- if gt (len $parts) 1 -}}
  {{- $first := index $parts 0 -}}
  {{- if or (eq $first "localhost") (contains "." $first) (contains ":" $first) -}}
    {{- $repositoryRegistry = $first -}}
    {{- $repositoryPath = rest $parts | join "/" -}}
  {{- end -}}
{{- end -}}
{{- $registry := $imageRegistry -}}
{{- if $repositoryRegistry -}}
  {{- $registry = $repositoryRegistry -}}
{{- end -}}
{{- if $globalRegistry -}}
  {{- $registry = $globalRegistry -}}
{{- end -}}
{{- $image := $repositoryPath -}}
{{- if $registry -}}
  {{- $image = printf "%s/%s" $registry $repositoryPath -}}
{{- end -}}
{{- $digest := .Values.image.digest | default "" -}}
{{- $tag := .Values.image.tag | default "" -}}
{{- if $digest -}}
{{- printf "%s@%s" $image $digest -}}
{{- else if $tag -}}
{{- printf "%s:%s" $image $tag -}}
{{- else -}}
{{- fail "image.tag or image.digest must be set" -}}
{{- end -}}
{{- end -}}

{{- define "bobs.imagePullSecrets" -}}
{{- $secrets := list -}}
{{- if .Values.global -}}
  {{- range .Values.global.imagePullSecrets | default list -}}
    {{- $secrets = append $secrets . -}}
  {{- end -}}
  {{- if .Values.global.imageCredentials -}}
    {{- $secrets = append $secrets (dict "name" (printf "%s-registry-cred" .Release.Name)) -}}
  {{- end -}}
{{- end -}}
{{- range .Values.imagePullSecrets | default list -}}
  {{- $secrets = append $secrets . -}}
{{- end -}}
{{- if $secrets }}
imagePullSecrets:
  {{- toYaml $secrets | nindent 2 }}
{{- end -}}
{{- end -}}
