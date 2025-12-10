SHELL := /bin/zsh

HAVE_BREW := $(shell command -v brew 2>/dev/null)

.PHONY: help deps deps-macos build build-release test clean server registry py-wheels

help:
	@echo "Targets:"
	@echo "  deps           Install system deps (macOS: brew)"
	@echo "  build          Debug build for Rust crates (excludes PyO3)"
	@echo "  build-release  Release build for Rust crates (excludes PyO3)"
	@echo "  py-wheels      Build Python bindings with maturin"
	@echo "  server         Build server binary (featureduck)"
	@echo "  registry       Build registry binary (featureduck-registry)"
	@echo "  test           Run Rust tests (all crates)"
	@echo "  clean          Clean target directory"

deps: deps-macos

deps-macos:
	@if [ -z "$(HAVE_BREW)" ]; then \
		echo "Homebrew not found. Install from https://brew.sh"; exit 1; \
	fi
	brew update
	brew install pkg-config openssl
	# Optional: python env for building wheels
	@echo "If using Python 3.13, enabling PyO3 ABI forward compat via env var"
	@mkdir -p .cargo
	@grep -q 'PYO3_USE_ABI3_FORWARD_COMPATIBILITY' .cargo/config.toml 2>/dev/null || \
	 (echo "[env]" && echo "PYO3_USE_ABI3_FORWARD_COMPATIBILITY=\"1\"") > .cargo/config.toml
	@echo "DuckDB library dir: $(DUCKDB_LIB_DIR)"

build:
	cargo build -p featureduck-core -p featureduck-delta -p featureduck-registry -p featureduck-server

build-release:
	cargo build -p featureduck-core -p featureduck-delta -p featureduck-registry -p featureduck-server --release

server:
	cargo build -p featureduck-server --release

registry:
	cargo build -p featureduck-registry --release

test:
	PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --workspace

clean:
	cargo clean

