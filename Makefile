.PHONY: build run lint format style_check test docker

build:
	cargo build --release

run:
	cargo run --release

lint:
	cargo clippy --all-targets --all-features -- -D warnings

format:
	cargo fmt --check

style_check: lint format

test:
	cargo test --all-targets --all-features

docker:
	docker build -t tmd-rs .
