cmake_minimum_required(VERSION 3.22)
project(engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ── Conan-generated package config ─────────────────────────────────────────────
set(CMAKE_NO_SYSTEM_FROM_IMPORTED ON)

find_package(gRPC REQUIRED)
find_package(protobuf REQUIRED)
find_package(spdlog REQUIRED)
find_package(GTest REQUIRED)

# Conan's build module may resolve protoc to a system copy (e.g. Homebrew)
# instead of the Conan package's own binary. Force the correct one.
set_target_properties(protobuf::protoc PROPERTIES
    IMPORTED_LOCATION "${protobuf_PACKAGE_FOLDER_RELEASE}/bin/protoc")

# ── Proto code generation ──────────────────────────────────────────────────────
set(PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../proto")
set(PROTO_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/proto-gen")
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})


set(PROTO_FILES
    ${PROTO_DIR}/raft.proto
    ${PROTO_DIR}/log.proto
)

set(PROTO_SRCS "")
set(PROTO_HDRS "")

foreach(PROTO_FILE ${PROTO_FILES})
    get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)

    set(PB_SRC  "${PROTO_GEN_DIR}/${PROTO_NAME}.pb.cc")
    set(PB_HDR  "${PROTO_GEN_DIR}/${PROTO_NAME}.pb.h")
    set(GRP_SRC "${PROTO_GEN_DIR}/${PROTO_NAME}.grpc.pb.cc")
    set(GRP_HDR "${PROTO_GEN_DIR}/${PROTO_NAME}.grpc.pb.h")

    add_custom_command(
        OUTPUT ${PB_SRC} ${PB_HDR} ${GRP_SRC} ${GRP_HDR}
        COMMAND $<TARGET_FILE:protobuf::protoc>
            --proto_path=${PROTO_DIR}
            --cpp_out=${PROTO_GEN_DIR}
            --grpc_out=${PROTO_GEN_DIR}
            --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
            ${PROTO_FILE}
        DEPENDS ${PROTO_FILE}
        COMMENT "Generating protobuf/gRPC for ${PROTO_NAME}.proto"
    )

    list(APPEND PROTO_SRCS ${PB_SRC} ${GRP_SRC})
    list(APPEND PROTO_HDRS ${PB_HDR} ${GRP_HDR})
endforeach()

add_library(proto-gen STATIC ${PROTO_SRCS} ${PROTO_HDRS})
target_include_directories(proto-gen PUBLIC ${PROTO_GEN_DIR})
target_link_libraries(proto-gen PUBLIC gRPC::grpc++ protobuf::protobuf)

# ── Core engine library (pure Raft layer) ──────────────────────────────────────
file(GLOB_RECURSE CORE_SRCS
    src/raft/*.cpp
    src/server/*.cpp
)

add_library(engine-core STATIC ${CORE_SRCS})
target_include_directories(engine-core PUBLIC src)
target_link_libraries(engine-core PUBLIC proto-gen spdlog::spdlog gRPC::grpc++)

# ── Main executable ───────────────────────────────────────────────────────────
add_executable(engine src/main.cpp)
target_link_libraries(engine PRIVATE engine-core)

# ── Unit tests ────────────────────────────────────────────────────────────────
file(GLOB TEST_SRCS tests/*.cpp)
add_executable(engine-tests ${TEST_SRCS})
target_link_libraries(engine-tests PRIVATE engine-core GTest::gtest GTest::gtest_main)

enable_testing()
add_test(NAME engine-tests COMMAND engine-tests)
