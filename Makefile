.PHONY: all production test itest docs clean build-image

DOCKER_TAG ?= schematizer-dev-$(USER)

all: production

production:
	@true

docs:
	tox -e docs

test:
	docker pull docker-dev.yelpcorp.com/mysql-testing:latest
	tox -e py27

itest: build-image
	paasta local-run -t
	tox -e acceptance

build-image:
	docker build -t $(DOCKER_TAG) .

clean:
	rm -rf docs/build
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	rm -rf .tox/

export GIT_SHA ?= $(shell git rev-parse --short HEAD)

docker_push:
	tox -e docker-push
