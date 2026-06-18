# Adherence Report: Fix BOBS Review Findings

Generated: 2026-06-10T09:25:44.676Z
Start SHA: `9192ff477b2576cfd6358f0ff3a32da2471dd595`
End SHA: `1b01b01be5f82373f55b5e2b57e9156f278b307d`

## Summary

| Metric | Value |
|--------|-------|
| Coverage (planned files touched) | 86% (25/29) |
| Precision (changes were planned) | 30% (24/79) |
| Planned files | 29 |
| Actual files changed | 79 |

## Missed (planned but not changed)

- chart/templates/_helpers.tpl
- /x@evil.com
- meta.json
- src/io/read_exact.rs

## Unplanned changes

- .gitignore
- Cargo.lock
- Dockerfile
- docs/book/.nojekyll
- docs/book/404.html
- docs/book/api-reference.html
- docs/book/architecture.html
- docs/book/ayu-highlight-3fdfc3ac.css
- docs/book/book-a0b12cfe.js
- docs/book/clipboard-1626706a.min.js
- docs/book/configuration.html
- docs/book/css/chrome-ae938929.css
- docs/book/css/general-2459343d.css
- docs/book/css/print-9e4910d8.css
- docs/book/css/variables-8adf115d.css
- docs/book/elasticlunr-ef4e11c1.min.js
- docs/book/favicon-8114d1fc.png
- docs/book/favicon-de23e50b.svg
- docs/book/fonts/OPEN-SANS-LICENSE.txt
- docs/book/fonts/SOURCE-CODE-PRO-LICENSE.txt
- docs/book/fonts/fonts-9644e21d.css
- docs/book/fonts/open-sans-v17-all-charsets-300-7736aa35.woff2
- docs/book/fonts/open-sans-v17-all-charsets-300italic-2c7b95c0.woff2
- docs/book/fonts/open-sans-v17-all-charsets-600-486c6759.woff2
- docs/book/fonts/open-sans-v17-all-charsets-600italic-1a3e8659.woff2
- docs/book/fonts/open-sans-v17-all-charsets-700-c22fe8c7.woff2
- docs/book/fonts/open-sans-v17-all-charsets-700italic-238ae959.woff2
- docs/book/fonts/open-sans-v17-all-charsets-800-3d2c812a.woff2
- docs/book/fonts/open-sans-v17-all-charsets-800italic-ba1521ec.woff2
- docs/book/fonts/open-sans-v17-all-charsets-italic-6c9463f7.woff2
- docs/book/fonts/open-sans-v17-all-charsets-regular-2e3b1d34.woff2
- docs/book/fonts/source-code-pro-v11-all-charsets-500-2bdd9410.woff2
- docs/book/getting-started.html
- docs/book/highlight-493f70e1.css
- docs/book/highlight-abc7f01d.js
- docs/book/index.html
- docs/book/introduction.html
- docs/book/key-behaviours.html
- docs/book/mark-09e88c2c.min.js
- docs/book/print.html
- docs/book/searcher-c2a407aa.js
- docs/book/searchindex-22639d04.js
- docs/book/standalone-benchmark.html
- docs/book/toc-953381c0.js
- docs/book/toc.html
- docs/book/tomorrow-night-4c0ae647.css
- docs/book/write-routing.html
- src/benchmark/config.rs
- src/benchmark/run.rs
- src/benchmark/schedule.rs
- src/bin/bobs-benchmark.rs
- src/io/tokio_fs.rs
- src/metadata.rs
- tests/integration.rs
- tests/standalone_benchmark.rs

## All actual changes

- .gitignore
- Cargo.lock
- Cargo.toml
- DESIGN.md
- Dockerfile
- TODO.md
- chart/templates/configmap.yaml
- chart/templates/ingress.yaml
- chart/templates/statefulset.yaml
- chart/values.yaml
- docs/book/.nojekyll
- docs/book/404.html
- docs/book/api-reference.html
- docs/book/architecture.html
- docs/book/ayu-highlight-3fdfc3ac.css
- docs/book/book-a0b12cfe.js
- docs/book/clipboard-1626706a.min.js
- docs/book/configuration.html
- docs/book/css/chrome-ae938929.css
- docs/book/css/general-2459343d.css
- docs/book/css/print-9e4910d8.css
- docs/book/css/variables-8adf115d.css
- docs/book/elasticlunr-ef4e11c1.min.js
- docs/book/favicon-8114d1fc.png
- docs/book/favicon-de23e50b.svg
- docs/book/fonts/OPEN-SANS-LICENSE.txt
- docs/book/fonts/SOURCE-CODE-PRO-LICENSE.txt
- docs/book/fonts/fonts-9644e21d.css
- docs/book/fonts/open-sans-v17-all-charsets-300-7736aa35.woff2
- docs/book/fonts/open-sans-v17-all-charsets-300italic-2c7b95c0.woff2
- docs/book/fonts/open-sans-v17-all-charsets-600-486c6759.woff2
- docs/book/fonts/open-sans-v17-all-charsets-600italic-1a3e8659.woff2
- docs/book/fonts/open-sans-v17-all-charsets-700-c22fe8c7.woff2
- docs/book/fonts/open-sans-v17-all-charsets-700italic-238ae959.woff2
- docs/book/fonts/open-sans-v17-all-charsets-800-3d2c812a.woff2
- docs/book/fonts/open-sans-v17-all-charsets-800italic-ba1521ec.woff2
- docs/book/fonts/open-sans-v17-all-charsets-italic-6c9463f7.woff2
- docs/book/fonts/open-sans-v17-all-charsets-regular-2e3b1d34.woff2
- docs/book/fonts/source-code-pro-v11-all-charsets-500-2bdd9410.woff2
- docs/book/getting-started.html
- docs/book/highlight-493f70e1.css
- docs/book/highlight-abc7f01d.js
- docs/book/index.html
- docs/book/introduction.html
- docs/book/key-behaviours.html
- docs/book/mark-09e88c2c.min.js
- docs/book/print.html
- docs/book/searcher-c2a407aa.js
- docs/book/searchindex-22639d04.js
- docs/book/standalone-benchmark.html
- docs/book/toc-953381c0.js
- docs/book/toc.html
- docs/book/tomorrow-night-4c0ae647.css
- docs/book/write-routing.html
- src/benchmark/config.rs
- src/benchmark/run.rs
- src/benchmark/schedule.rs
- src/bin/bobs-benchmark.rs
- src/cleanup.rs
- src/config.rs
- src/error.rs
- src/http/mod.rs
- src/io/mod.rs
- src/io/ring_pool.rs
- src/io/tokio_fs.rs
- src/io/uring_fs.rs
- src/lib.rs
- src/main.rs
- src/manager.rs
- src/metadata.rs
- src/metadata/uring.rs
- src/observability.rs
- src/spool/lifecycle.rs
- src/spool/reader.rs
- src/spool/writer.rs
- src/time.rs
- tests/integration.rs
- tests/observability.rs
- tests/standalone_benchmark.rs