.PHONY: all build test clean help

all: build

build:
	@echo "==> Building coordinator..."
	mvn -pl server -am package -DskipTests -q

test:
	@echo "==> Running tests..."
	mvn test -q

clean:
	mvn clean -q

help:
	@echo "Targets:"
	@echo "  build    Build coordinator"
	@echo "  test     Run all tests"
	@echo "  clean    Clean build artifacts"
