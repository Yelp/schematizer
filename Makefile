REBUILD_FLAG =

.PHONY: help all production clean clean-build clean-pyc clean-pycache clean-build clean-docs clean-tox lint test docs install-hooks

help:
	@echo "clean - remove artifacts"
	@echo "debug - run tests and allow interactive breaks on `ipbd.set_trace()`"
	@echo "docs - generate Sphinx HTML documentation, including API docs"
	@echo "lint - check style with flake8"
	@echo "test - run tests quickly with the default Python"
	@echo "itest - cook the image and do paasta local run acceptance tests"
	@echo "install-hooks - Install the pre-commit hooks"

all: production install-hooks

production:
	@true

clean: clean-build clean-pyc clean-pycache clean-docs clean-tox

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr *.egg-info

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +

clean-pycache:
	find . -name '__pycache__' -exec rm -rf {} +

clean-docs:
	rm -rf docs/build/*

clean-tox:
	rm -rf .tox/

docs:
	tox -e docs

lint:
	tox -e style

test:
	tox $(REBUILD_FLAG)

debug:
	tox $(REBUILD_FLAG) -- -s

itest: cook-image
	paasta local-run -s schematizer -t
	tox -e acceptance

DOCKER_TAG ?= schematizer-dev-$(USER)

cook-image:
	docker build -t $(DOCKER_TAG) .

export GIT_SHA ?= $(shell git rev-parse --short HEAD)

docker_push:
	tox -e docker-push

install-hooks:
	tox -e pre-commit -- install -f --install-hooks
