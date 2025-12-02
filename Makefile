CURRENT_DATE := $(shell date +%Y%m%d)
TAG ?= v1.0
REPO?=rvm:5100/clickhouse/mcp-clickhouse

.PHONY: build
build: build-image

.PHONY: build-image
build-image: ## Build the Docker image.
	DOCKER_BUILDKIT=1 docker build -t mcp-clickhouse:$(TAG) .

.PHONY: build-image-date
build-image-date: ## Build the Docker image.
	DOCKER_BUILDKIT=1 docker build -t mcp-clickhouse:$(TAG)-$(CURRENT_DATE) .