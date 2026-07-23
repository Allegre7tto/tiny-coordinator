.PHONY: all build build-rust build-java test test-rust test-java clean help

# ─── Default ──────────────────────────────────────────────────────────────────
all: build

# ─── Build ────────────────────────────────────────────────────────────────────
build: build-rust build-java

build-rust:
	@echo "==> Building Rust engine..."
	cd core && cargo build --release

build-java:
	@echo "==> Building Java coordinator..."
	cd coordinator && mvn package -DskipTests -q

# ─── Test ─────────────────────────────────────────────────────────────────────
test: test-rust test-java

test-rust:
	@echo "==> Running Rust tests..."
	cd core && cargo test

test-java:
	@echo "==> Running Java tests..."
	cd coordinator && mvn test -q

# ─── Clean ────────────────────────────────────────────────────────────────────
clean:
	cd core && cargo clean
	cd coordinator && mvn clean -q
	rm -rf core-cpp/build

# ─── Help ─────────────────────────────────────────────────────────────────────
help:
	@echo "Targets:"
	@echo "  build          Build Rust engine + Java coordinator"
	@echo "  build-rust     Build Rust engine only"
	@echo "  build-java     Build Java coordinator only"
	@echo "  test           Run all tests"
	@echo "  test-rust      Run Rust tests"
	@echo "  test-java      Run Java tests"
	@echo "  clean          Clean all build artifacts"
