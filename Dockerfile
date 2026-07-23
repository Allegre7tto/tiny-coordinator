FROM maven:3-eclipse-temurin-21 AS builder

WORKDIR /build
COPY .mvn/ .mvn/
COPY pom.xml .
COPY proto/pom.xml proto/pom.xml
COPY raft/pom.xml raft/pom.xml
COPY runtime/pom.xml runtime/pom.xml
COPY server/pom.xml server/pom.xml
COPY testkit/pom.xml testkit/pom.xml
COPY proto/src/ proto/src/
COPY raft/src/ raft/src/
COPY runtime/src/ runtime/src/
COPY server/src/ server/src/
RUN mvn -pl server -am package -DskipTests -q

FROM eclipse-temurin:21-jre

COPY --from=builder /build/server/target/quarkus-app/ /app/

EXPOSE 9000 9001

ENV RAFT_NODE_ID=1 \
    RAFT_DATA_DIR=/data \
    RAFT_MEMBERS=1=coordinator-1:9000,2=coordinator-2:9000,3=coordinator-3:9000 \
    ENGINE_GRPC_PORT=9000 \
    ENGINE_MGMT_PORT=9001 \
    ENGINE_JSON_LOG=true

ENTRYPOINT ["java", "-jar", "/app/quarkus-run.jar"]
