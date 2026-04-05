.PHONY: all build build-cpp build-java test test-cpp test-java clean help

# ─── Default ──────────────────────────────────────────────────────────────────
all: build

# ─── Build ────────────────────────────────────────────────────────────────────
build: build-cpp build-java

build-cpp:
	@echo "==> Building C++ engine..."
	cd core && conan install . --build=missing -of=build -s compiler.cppstd=20 && cmake --preset conan-release && cmake --build build --config Release

build-java:
	@echo "==> Building Java coordinator..."
	cd coordinator && mvn package -DskipTests -q

# ─── Test ─────────────────────────────────────────────────────────────────────
test: test-cpp test-java

test-cpp:
	@echo "==> Running C++ tests..."
	cd core/build && ctest --output-on-failure

test-java:
	@echo "==> Running Java tests..."
	cd coordinator && mvn test -q

# ─── Clean ────────────────────────────────────────────────────────────────────
clean:
	rm -rf core/build
	cd coordinator && mvn clean -q

# ─── Help ─────────────────────────────────────────────────────────────────────
help:
	@echo "Targets:"
	@echo "  build          Build C++ engine + Java coordinator"
	@echo "  build-cpp      Build C++ engine only"
	@echo "  build-java     Build Java coordinator only"
	@echo "  test           Run all tests"
	@echo "  test-cpp       Run C++ unit tests"
	@echo "  test-java      Run Java unit tests"
	@echo "  clean          Clean all build artifacts"
