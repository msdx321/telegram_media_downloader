TEST_ARTIFACTS ?= /tmp/coverage

.PHONY: install dev_install lint format type_check test

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

type_check:
	uv run mypy media_downloader.py utils module --ignore-missing-imports

style_check: lint format type_check

test:
	uv run pytest \
		--cov media_downloader \
		--cov utils \
		--cov-report term-missing \
		--cov-report html:${TEST_ARTIFACTS} \
		--junit-xml=${TEST_ARTIFACTS}/media-downloader.xml \
		tests/
