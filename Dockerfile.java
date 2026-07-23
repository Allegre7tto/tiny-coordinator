# ── Rust builder: compile Raft engine shared library ──────────────────────────
FROM rust:1.89-slim AS rust-builder

RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY proto/ proto/
COPY core/ core/

WORKDIR /build/core
RUN cargo build --release -p jni

# ── Java builder: compile coordinator ──────────────────────────────────────────
FROM maven:3-eclipse-temurin-21 AS java-builder

WORKDIR /build
COPY proto/coordinator.proto coordinator/src/main/proto/
COPY coordinator/pom.xml coordinator/pom.xml
COPY coordinator/src/ coordinator/src/
COPY --from=rust-builder /build/core/target/release/libjni.so /usr/local/lib/libjni.so

WORKDIR /build/coordinator
RUN mvn package -DskipTests -q

# ── Runtime: JVM with Rust shared library ──────────────────────────────────────
FROM eclipse-temurin:21-jre

COPY --from=rust-builder /build/core/target/release/libjni.so /usr/local/lib/libjni.so
COPY --from=java-builder /build/coordinator/target/quarkus-app/ /app/

EXPOSE 7000 9000 9001

ENV RAFT_ID=0 \
    RAFT_DATA_DIR=/data \
    RAFT_PEERS=coordinator-0:7000,coordinator-1:7000,coordinator-2:7000 \
    RAFT_PORT=7000 \
    ENGINE_GRPC_PORT=9000 \
    ENGINE_MGMT_PORT=9001 \
    ENGINE_JSON_LOG=true

ENV LD_LIBRARY_PATH=/usr/local/lib

ENTRYPOINT ["java", "-jar", "/app/quarkus-run.jar"]
