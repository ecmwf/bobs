<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Introduction

BOBS (Big-Object Buffered Storage) is a streaming spool service designed to buffer large producer responses for consumption by slow or remote readers.

## Why BOBS?

Data producers like HPC tasks or local processing applications often generate data faster than remote consumers can ingest it over a Wide Area Network (WAN). Direct connections between such producers and consumers can lead to backpressure issues or inefficient resource utilization.

BOBS provides a high-performance, temporary storage layer that:
- Allows producers to append data at local speeds.
- Enables a single consumer to use parallel connections when needed.
- Supports byte-range requests and real-time streaming (follow mode).
- Automatically cleans up data based on lifecycle events and timeouts.

## Deployment Model

BOBS is designed for containerized environments, specifically Kubernetes. 
- **K8s Pods**: Typically deployed as a set of pods with local persistent volumes for high-speed I/O.
- **DNS Routing**: Clients use DNS to route requests to specific BOBS instances.
- **preferLocal**: In multi-node setups, routing logic often prefers local pods to minimize latency between the producer and the buffer.
