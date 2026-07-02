FROM docker.io/library/rust:1.93-slim-bookworm AS builder
WORKDIR /build
ARG FEATURES=""
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN if [ -n "$FEATURES" ]; then \
    cargo build --release --locked --bins --features "$FEATURES"; \
  else \
    cargo build --release --locked --bins; \
  fi

FROM docker.io/library/debian:bookworm-slim AS release
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && groupadd --gid 10001 bobs \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin bobs \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/bobs /usr/local/bin/bobs
COPY --from=builder /build/target/release/bobs-benchmark /usr/local/bin/bobs-benchmark
USER 10001:10001
EXPOSE 3000
CMD ["bobs"]

FROM docker.io/library/debian:bookworm-slim AS debug
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates bash \
    && groupadd --gid 10001 bobs \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin bobs \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/bobs /usr/local/bin/bobs
COPY --from=builder /build/target/release/bobs-benchmark /usr/local/bin/bobs-benchmark
USER 10001:10001
EXPOSE 3000
CMD ["bobs"]
