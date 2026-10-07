<!-- SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF) -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Changelog

## 0.1.14 — 2026-10-07

- Removed foreground filesystem syncs and the `fsync_enabled` configuration option.
- Completed spools now record exact length and XXH3-64 checksum atomically and are fully verified before any response headers or body bytes are sent.
- Completed spools created by older releases without integrity metadata are quarantined during recovery and return HTTP 410 `result_lost` after upgrade.
- The coalesced background filesystem flush always runs after completion; `async_sync_delay_ms: 0` means flush immediately in the background.
