FROM docker.io/library/rust:1.90-slim AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release

FROM docker.io/library/debian:bookworm-slim AS release
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/bobs /usr/local/bin/bobs
EXPOSE 3000
CMD ["bobs"]

FROM docker.io/library/debian:bookworm-slim AS debug
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates bash && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/bobs /usr/local/bin/bobs
EXPOSE 3000
CMD ["bobs"]
