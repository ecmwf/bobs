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

{{/*
Resolve the StatefulSet governing Service. An empty name keeps the historical
<fullname>-svc default when this chart manages the Service. Disabled management
requires an explicit existing headless Service in the release namespace.
*/}}
{{- define "bobs.headlessServiceName" -}}
{{- $rawName := .Values.headlessService.name | default "" -}}
{{- $name := $rawName | trim -}}
{{- if ne $rawName $name -}}
{{- fail "headlessService.name must be a valid DNS-1123 Service name (lowercase alphanumeric or '-', at most 63 characters)" -}}
{{- end -}}
{{- if $name -}}
{{- if or (gt (len $name) 63) (not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" $name)) -}}
{{- fail "headlessService.name must be a valid DNS-1123 Service name (lowercase alphanumeric or '-', at most 63 characters)" -}}
{{- end -}}
{{- $name -}}
{{- else if .Values.headlessService.enabled -}}
{{- printf "%s-svc" (include "bobs.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- fail "headlessService.name must be a non-empty external governing Service name when headlessService.enabled=false" -}}
{{- end -}}
{{- end -}}

{{/* BOBS mounts data_dir as a directory and cannot consume a raw block device. */}}
{{- define "bobs.validatePersistence" -}}
{{- if ne (.Values.persistence.volumeMode | default "") "Filesystem" -}}
{{- fail "persistence.volumeMode must be Filesystem because BOBS requires a filesystem directory; raw Block volumes are unsupported" -}}
{{- end -}}
{{- end -}}

{{/* Keep Helm's contract consistent with Config::validate. */}}
{{- define "bobs.validateConfigBounds" -}}
{{- if gt (int64 .Values.config.page_size) (int64 .Values.config.max_spool_bytes) -}}
{{- fail "config.page_size must not exceed config.max_spool_bytes" -}}
{{- end -}}
{{- end -}}

{{/* Validate chart-wide invariants while resolving the StatefulSet serviceName. */}}
{{- define "bobs.governingServiceName" -}}
{{- include "bobs.validatePersistence" . -}}
{{- include "bobs.headlessServiceName" . -}}
{{- end -}}

{{/* Keep the fsGroup fallback outside YAML so an empty context still mounts writable PVCs. */}}
{{- define "bobs.podSecurityContext" -}}
{{- if .Values.podSecurityContext -}}
{{- toYaml .Values.podSecurityContext -}}
{{- else -}}
{{- toYaml (dict "fsGroup" 10001) -}}
{{- end -}}
{{- end -}}

{{/* Render the optional PVC storageClassName without duplicating YAML keys. */}}
{{- define "bobs.storageClassName" -}}
{{- if and .Values.persistence.storageClass (ne .Values.persistence.storageClass "-") -}}
storageClassName: {{ .Values.persistence.storageClass | quote }}
{{- else if eq .Values.persistence.storageClass "-" -}}
storageClassName: ""
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

{{/* Render community ingress-nginx resources, including per-pod forwarded prefixes. */}}
{{- define "bobs.communityIngresses" -}}
{{- $fullName := include "bobs.fullname" . -}}
{{- $routeName := required "config.route_name must be set" .Values.config.route_name -}}
{{- $port := .Values.service.port -}}
{{- $host := printf "%s.%s" .Values.config.host_prefix .Values.config.domain -}}
{{- $userAnnotations := .Values.ingress.annotations | default dict -}}
{{- /* Strip NGINX Inc / Bologna-only keys from community ingress resources. */ -}}
{{- $filtered := dict -}}
{{- range $key, $value := $userAnnotations -}}
  {{- if not (or (hasPrefix "nginx.org/" $key) (hasPrefix "dns.operators.ecmwf.int/" $key)) -}}
    {{- $_ := set $filtered $key $value -}}
  {{- end -}}
{{- end -}}
{{- $secureDefaults := dict
  "nginx.ingress.kubernetes.io/ssl-redirect" "true"
  "nginx.ingress.kubernetes.io/force-ssl-redirect" "true"
  "nginx.ingress.kubernetes.io/use-regex" "true"
  "nginx.ingress.kubernetes.io/rewrite-target" "/api/v1/read/$2" -}}
{{- $annotations := mergeOverwrite (deepCopy $secureDefaults) $filtered -}}
{{ print "\n" -}}
# Community ingress-nginx's native x-forwarded-prefix annotation is static per
# Ingress, so forwarded-prefix mode renders one Ingress per replica. The regex
# accepts short public URLs and redirected public /api/v1/read URLs.
{{- if .Values.ingress.forwardedPrefix.enabled }}
{{- range $index, $_ := until (int .Values.replicaCount) }}
{{- $podAnnotations := deepCopy $annotations -}}
{{- $_ := set $podAnnotations "nginx.ingress.kubernetes.io/x-forwarded-prefix" (printf "/%s-%d" $routeName $index) }}
{{ if gt $index 0 }}
---
{{ end }}
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: {{ (printf "%s-%d" $fullName $index) | trunc 63 | trimSuffix "-" | quote }}
  labels:
    app.kubernetes.io/name: '{{ include "bobs.name" $ }}'
    app.kubernetes.io/instance: '{{ $.Release.Name }}'
  annotations:
    {{- toYaml $podAnnotations | nindent 4 }}
spec:
  {{- if $.Values.ingress.className }}
  ingressClassName: {{ $.Values.ingress.className | quote }}
  {{- end }}
  rules:
    - host: {{ $host | quote }}
      http:
        paths:
          - path: '/{{ $routeName }}-{{ $index }}/(api/v1/read/|api/v1/)?([0-9a-zA-Z-]+)$'
            pathType: ImplementationSpecific
            backend:
              service:
                name: '{{ $fullName }}-{{ $index }}'
                port:
                  number: {{ $port }}
  {{- if $.Values.ingress.tls }}
  tls:
    {{- toYaml $.Values.ingress.tls | nindent 4 }}
  {{- end }}
{{- end }}
{{- else }}
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: '{{ $fullName }}'
  labels:
    app.kubernetes.io/name: '{{ include "bobs.name" . }}'
    app.kubernetes.io/instance: '{{ .Release.Name }}'
  annotations:
    {{- toYaml $annotations | nindent 4 }}
spec:
  {{- if .Values.ingress.className }}
  ingressClassName: {{ .Values.ingress.className | quote }}
  {{- end }}
  rules:
    - host: {{ $host | quote }}
      http:
        paths:
          {{- range $index, $_ := until (int $.Values.replicaCount) }}
          - path: '/{{ $routeName }}-{{ $index }}/(api/v1/read/|api/v1/)?([0-9a-zA-Z-]+)$'
            pathType: ImplementationSpecific
            backend:
              service:
                name: '{{ $fullName }}-{{ $index }}'
                port:
                  number: {{ $port }}
          {{- end }}
  {{- if .Values.ingress.tls }}
  tls:
    {{- toYaml .Values.ingress.tls | nindent 4 }}
  {{- end }}
{{- end }}
{{- end -}}
