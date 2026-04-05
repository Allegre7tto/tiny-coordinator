# ── Build stage ────────────────────────────────────────────────────────────────
FROM ubuntu:24.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential git curl unzip ca-certificates cmake python3 python3-pip pipx \
    && pipx install conan \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.local/bin:${PATH}"

RUN conan profile detect

WORKDIR /build
COPY proto/ proto/
COPY core/ core/

WORKDIR /build/core
RUN conan install . --build=missing -of=build \
    && cmake --preset conan-release \
    && cmake --build build --config Release

# ── Runtime stage ──────────────────────────────────────────────────────────────
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/core/build/engine /usr/local/bin/engine

RUN mkdir -p /data

EXPOSE 7000

ENTRYPOINT ["engine"]
CMD ["0", "/data", "0.0.0.0:7000"]
