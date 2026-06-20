TEST_ARTIFACTS ?= /tmp/coverage

.PHONY: install dev_install lint format test

install:
	uv sync --no-dev
	uv run python gen_filter_cache.py

dev_install:
	uv sync
	uv run python gen_filter_cache.py

lint:
	uv run ruff check .

format:
	uv run ruff format --check .

style_check: lint format

test:
	uv run pytest \
		--cov media_downloader \
		--cov utils \
		--cov-report term-missing \
		--cov-report html:${TEST_ARTIFACTS} \
		--junit-xml=${TEST_ARTIFACTS}/media-downloader.xml \
		tests/
