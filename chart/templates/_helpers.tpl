{{/* Validate operator-provided names before they reach Kubernetes metadata. */}}
{{- define "bobs.validateDnsLabel" -}}
{{- $field := index . "field" -}}
{{- $rawName := index . "name" -}}
{{- $name := $rawName | trim -}}
{{- if or (ne $rawName $name) (gt (len $name) 63) (not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" $name)) -}}
{{- fail (printf "%s must be a valid DNS-1123 label (lowercase alphanumeric or '-', at most 63 characters)" $field) -}}
{{- end -}}
{{- $name -}}
{{- end -}}

{{- define "bobs.name" -}}
{{- $override := .Values.nameOverride | default "" -}}
{{- if $override -}}
{{- include "bobs.validateDnsLabel" (dict "field" "nameOverride" "name" $override) -}}
{{- else -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/* Map Helm-valid dotted release names to collision-resistant DNS labels. */}}
{{- define "bobs.releaseName" -}}
{{- $rawName := .Release.Name -}}
{{- if contains "." $rawName -}}
{{- printf "%s-%s" ($rawName | replace "." "-") (sha256sum $rawName | trunc 8) -}}
{{- else -}}
{{- $rawName -}}
{{- end -}}
{{- end -}}

{{/*
Shorten a DNS label without losing all entropy from the truncated tail. The
eight-character digest helps distinguish long bases that share the retained prefix.
Callers may then append a stable suffix within the same length budget.
*/}}
{{- define "bobs.boundedName" -}}
{{- $base := index . "base" -}}
{{- $maxLength := int (index . "maxLength") -}}
{{- if lt $maxLength 10 -}}
{{- fail "internal chart error: bounded DNS label length must leave room for a digest" -}}
{{- end -}}
{{- if gt (len $base) $maxLength -}}
{{- $digest := sha256sum $base | trunc 8 -}}
{{- $prefixLength := sub $maxLength 9 -}}
{{- $prefix := $base | trunc (int $prefixLength) | trimSuffix "-" -}}
{{- printf "%s-%s" $prefix $digest -}}
{{- else -}}
{{- $base -}}
{{- end -}}
{{- end -}}

{{/* Append a suffix while preserving it and keeping the result within 63 bytes. */}}
{{- define "bobs.nameWithSuffix" -}}
{{- $base := index . "base" -}}
{{- $suffix := index . "suffix" -}}
{{- $baseBudget := sub 63 (len $suffix) -}}
{{- if lt $baseBudget 10 -}}
{{- fail "internal chart error: generated Kubernetes name suffix is too long" -}}
{{- end -}}
{{- printf "%s%s" (include "bobs.boundedName" (dict "base" $base "maxLength" $baseBudget)) $suffix -}}
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
{{- $override := .Values.fullnameOverride | default "" -}}
{{- if $override -}}
{{- include "bobs.validateDnsLabel" (dict "field" "fullnameOverride" "name" $override) -}}
{{- else -}}
{{- $name := include "bobs.name" . -}}
{{- $releaseName := include "bobs.releaseName" . -}}
{{- if contains $name $releaseName -}}
{{- $releaseName | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" $releaseName $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Reserve enough space for data-<statefulset>-<10-digit ordinal>, the longest
controller-generated DNS label for Kubernetes' signed 32-bit replica field.
Long names retain an eight-character digest to reduce prefix collisions.
*/}}
{{- define "bobs.statefulsetName" -}}
{{- include "bobs.boundedName" (dict "base" (include "bobs.fullname" .) "maxLength" 47) -}}
{{- end -}}

{{/* The StatefulSet pod name is also the corresponding per-pod Service name. */}}
{{- define "bobs.podName" -}}
{{- $root := index . "root" -}}
{{- $ordinal := index . "ordinal" -}}
{{- include "bobs.nameWithSuffix" (dict "base" (include "bobs.statefulsetName" $root) "suffix" (printf "-%d" $ordinal)) -}}
{{- end -}}

{{/*
Resolve the StatefulSet governing Service. An empty name keeps the historical
<fullname>-svc default when this chart manages the Service. Disabled management
requires an explicit existing headless Service in the release namespace.
*/}}
{{- define "bobs.headlessServiceName" -}}
{{- $rawName := .Values.headlessService.name | default "" -}}
{{- if $rawName -}}
{{- include "bobs.validateDnsLabel" (dict "field" "headlessService.name" "name" $rawName) -}}
{{- else if .Values.headlessService.enabled -}}
{{- include "bobs.nameWithSuffix" (dict "base" (include "bobs.fullname" .) "suffix" "-svc") -}}
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

{{/* Restrict chart-managed data volume mounts to a normalized application-owned subtree. */}}
{{- define "bobs.dataDir" -}}
{{- $dataDir := .Values.config.data_dir | default "" | toString -}}
{{- $cleanDataDir := clean $dataDir -}}
{{- $isSupported := or (eq $dataDir "/var/lib/bobs") (hasPrefix "/var/lib/bobs/" $dataDir) -}}
{{- if or (ne $dataDir $cleanDataDir) (not $isSupported) -}}
{{- fail "config.data_dir must be /var/lib/bobs or a normalized descendant (no '.', '..', repeated '/', or trailing '/') because the chart mounts it as the data volume" -}}
{{- end -}}
{{- $dataDir -}}
{{- end -}}

{{/* Validate chart-level config invariants before rendering. */}}
{{- define "bobs.validateConfigBounds" -}}
{{- $_ := include "bobs.dataDir" . -}}
{{- $_ := include "bobs.routeName" . -}}
{{- if gt (int64 .Values.config.page_size) (int64 .Values.config.max_spool_bytes) -}}
{{- fail "config.page_size must not exceed config.max_spool_bytes" -}}
{{- end -}}
{{- if gt (int64 .Values.config.max_live_spools) 65536 -}}
{{- fail "config.max_live_spools must not exceed 65536" -}}
{{- end -}}
{{- if and .Values.config.metrics.enabled (eq (int64 .Values.config.port) (int64 .Values.config.metrics.port)) -}}
{{- fail "config.metrics.port must differ from config.port when metrics are enabled" -}}
{{- end -}}
{{- end -}}

{{/* Restrict route_name to one literal URL/NGINX path segment. */}}
{{- define "bobs.routeName" -}}
{{- $routeName := .Values.config.route_name | default "" | toString -}}
{{- if or (gt (len $routeName) 63) (not (regexMatch "^[A-Za-z0-9]([A-Za-z0-9_-]{0,61}[A-Za-z0-9])?$" $routeName)) -}}
{{- fail "config.route_name must be 1-63 characters, contain only ASCII letters, digits, '_' or '-', and start and end with an alphanumeric character" -}}
{{- end -}}
{{- $routeName -}}
{{- end -}}

{{/* Reject governing Service names that alias another Service in this release. */}}
{{- define "bobs.validateServiceNameCollisions" -}}
{{- $root := index . "root" -}}
{{- $governingName := index . "governingName" -}}
{{- $seen := dict -}}
{{- $mainName := include "bobs.fullname" $root -}}
{{- $_ := set $seen $mainName "main Service" -}}
{{- range $ordinal, $_ := until (int $root.Values.replicaCount) -}}
{{- $podName := include "bobs.podName" (dict "root" $root "ordinal" $ordinal) -}}
{{- if hasKey $seen $podName -}}
{{- fail (printf "generated Service name %q collides with the %s" $podName (get $seen $podName)) -}}
{{- end -}}
{{- $_ := set $seen $podName (printf "per-pod Service for ordinal %d" $ordinal) -}}
{{- end -}}
{{- if hasKey $seen $governingName -}}
{{- fail (printf "headlessService.name resolves to %q and collides with the generated %s" $governingName (get $seen $governingName)) -}}
{{- end -}}
{{- end -}}

{{/* Validate chart-wide invariants while resolving the StatefulSet serviceName. */}}
{{- define "bobs.governingServiceName" -}}
{{- include "bobs.validatePersistence" . -}}
{{- $name := include "bobs.headlessServiceName" . -}}
{{- include "bobs.validateServiceNameCollisions" (dict "root" . "governingName" $name) -}}
{{- $name -}}
{{- end -}}

{{/* Render mutually exclusive environment variable sources as one YAML entry. */}}
{{- define "bobs.deploymentEnvEntry" -}}
{{- if .Values.observability.deploymentEnv -}}
value: {{ .Values.observability.deploymentEnv | quote }}
{{- else -}}
valueFrom:
  configMapKeyRef:
    name: {{ .Values.observability.deploymentEnvConfigMapName | default (printf "%s-observability-env" .Release.Name) | quote }}
    key: {{ .Values.observability.deploymentEnvConfigMapKey | default "BOBS_DEPLOYMENT_ENV" | quote }}
    optional: true
{{- end -}}
{{- end -}}

{{- define "bobs.internalBaseUrlEntry" -}}
{{- if .Values.internalBaseUrlTemplate -}}
value: {{ tpl .Values.internalBaseUrlTemplate . | quote }}
{{- else -}}
value: '{{ printf "http://%s-{ordinal}.%s:%d/api/v1" (include "bobs.statefulsetName" .) (include "bobs.headlessServiceName" .) (int .Values.service.port) }}'
{{- end -}}
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
helm.sh/chart: {{ printf "%s-%s" .Chart.Name (.Chart.Version | replace "+" "_") | quote }}
app.kubernetes.io/name: {{ include "bobs.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | toString | quote }}
app.kubernetes.io/version: {{ .Chart.AppVersion | toString | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | toString | quote }}
{{- end -}}

{{- define "bobs.selectorLabels" -}}
app.kubernetes.io/name: {{ include "bobs.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | toString | quote }}
{{- end -}}

{{/* Merge ServiceMonitor labels once and force every value to a YAML string. */}}
{{- define "bobs.serviceMonitorLabels" -}}
{{- $labels := dict
  "helm.sh/chart" (printf "%s-%s" .Chart.Name (.Chart.Version | replace "+" "_"))
  "app.kubernetes.io/name" (include "bobs.name" .)
  "app.kubernetes.io/instance" (.Release.Name | toString)
  "app.kubernetes.io/version" (.Chart.AppVersion | toString)
  "app.kubernetes.io/managed-by" (.Release.Service | toString) -}}
{{- range $key, $value := .Values.config.metrics.serviceMonitor.labels | default dict -}}
{{- $_ := set $labels $key ($value | toString) -}}
{{- end -}}
{{- range $key := keys $labels | sortAlpha }}
{{ $key }}: {{ get $labels $key | toString | quote }}
{{- end -}}
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

{{/* Render one stable Service for every StatefulSet pod. */}}
{{- define "bobs.podServices" -}}
{{ print "\n" -}}
{{- $port := .Values.service.port -}}
{{- range $ordinal, $_ := until (int .Values.replicaCount) }}
{{- if gt $ordinal 0 }}
---
{{- end }}
apiVersion: v1
kind: Service
metadata:
  name: '{{ include "bobs.podName" (dict "root" $ "ordinal" $ordinal) }}'
  labels:
    {{- include "bobs.selectorLabels" $ | nindent 4 }}
spec:
  ports:
    - port: {{ $port }}
      targetPort: http
      protocol: TCP
  selector:
    statefulset.kubernetes.io/pod-name: '{{ include "bobs.podName" (dict "root" $ "ordinal" $ordinal) }}'
{{- end }}
{{- end -}}

{{/* Render community ingress-nginx resources, including per-pod forwarded prefixes. */}}
{{- define "bobs.communityIngresses" -}}
{{- $fullName := include "bobs.fullname" . -}}
{{- $routeName := include "bobs.routeName" . -}}
{{- $routePattern := regexQuoteMeta $routeName -}}
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
{{- $podName := include "bobs.podName" (dict "root" $ "ordinal" $index) -}}
{{- $podAnnotations := deepCopy $annotations -}}
{{- $_ := set $podAnnotations "nginx.ingress.kubernetes.io/x-forwarded-prefix" (printf "/%s-%d" $routeName $index) }}
{{ if gt $index 0 }}
---
{{ end }}
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: {{ $podName | quote }}
  labels:
    {{- include "bobs.selectorLabels" $ | nindent 4 }}
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
          - path: {{ printf "/%s-%d/(api/v1/read/|api/v1/)?([0-9a-zA-Z-]+)$" $routePattern $index | quote }}
            pathType: ImplementationSpecific
            backend:
              service:
                name: {{ $podName | quote }}
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
  name: {{ $fullName | quote }}
  labels:
    {{- include "bobs.selectorLabels" . | nindent 4 }}
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
          {{- $podName := include "bobs.podName" (dict "root" $ "ordinal" $index) }}
          - path: {{ printf "/%s-%d/(api/v1/read/|api/v1/)?([0-9a-zA-Z-]+)$" $routePattern $index | quote }}
            pathType: ImplementationSpecific
            backend:
              service:
                name: {{ $podName | quote }}
                port:
                  number: {{ $port }}
          {{- end }}
  {{- if .Values.ingress.tls }}
  tls:
    {{- toYaml .Values.ingress.tls | nindent 4 }}
  {{- end }}
{{- end }}
{{ print "\n" -}}
{{- end -}}
