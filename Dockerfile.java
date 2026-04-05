# ── Build stage (native image) ─────────────────────────────────────────────────
FROM ghcr.io/graalvm/native-image-community:21 AS builder

RUN microdnf install -y maven findutils && microdnf clean all

WORKDIR /build

COPY proto/ proto/
COPY coordinator/pom.xml coordinator/pom.xml

WORKDIR /build/coordinator
RUN mvn dependency:go-offline -q

COPY coordinator/src/ src/
RUN rm -rf src/main/proto/engine && ln -s /build/proto src/main/proto/engine

# Quarkus native build: produces a single static binary
RUN mvn package -DskipTests -Dnative \
    -Dquarkus.native.container-build=false \
    -Dquarkus.native.additional-build-args="--static","--libc=musl" \
    -q

# ── Runtime stage (distroless — no OS, no shell, ~3MB base) ────────────────────
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /build/coordinator/target/*-runner /app/coordinator

EXPOSE 9000 9001

ENV ENGINE_CPP_HOST=engine-cpp \
    ENGINE_CPP_PORT=7000 \
    ENGINE_GRPC_PORT=9000 \
    ENGINE_MGMT_PORT=9001 \
    ENGINE_JSON_LOG=true

ENTRYPOINT ["/app/coordinator"]
