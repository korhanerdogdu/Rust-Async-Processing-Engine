# Multi-stage build for both processes.
#   docker compose --profile apps up -d --build
# Targets: `api-gateway`, `worker-engine`.

# ---- build stage ---------------------------------------------------------
FROM rust:1.91-bookworm AS builder
WORKDIR /src
# protoc comes from the vendored `protoc-bin-vendored` crate; nothing to install.
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p api-gateway -p worker-engine \
 && mkdir -p /out \
 && cp target/release/api-gateway target/release/worker-engine /out/

# ---- api-gateway runtime ----------------------------------------------------
FROM debian:bookworm-slim AS api-gateway
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home /app engine
COPY --from=builder /out/api-gateway /usr/local/bin/api-gateway
USER engine
WORKDIR /app
EXPOSE 8080 50051
ENTRYPOINT ["/usr/local/bin/api-gateway"]

# ---- worker-engine runtime (with Chromium for html_to_pdf) -----------------
FROM debian:bookworm-slim AS worker-engine
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates chromium fonts-dejavu-core fonts-liberation \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home /app engine \
 && mkdir -p /var/lib/engine/artifacts && chown -R engine /var/lib/engine
COPY --from=builder /out/worker-engine /usr/local/bin/worker-engine
USER engine
WORKDIR /app
ENV PDF_BROWSER_PATH=/usr/bin/chromium ARTIFACT_DIR=/var/lib/engine/artifacts
EXPOSE 50052
ENTRYPOINT ["/usr/local/bin/worker-engine"]
