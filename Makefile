.PHONY: all production test itest docs clean

all: production

production:
	@true

docs:
	tox -e docs

test:
	docker pull docker-dev.yelpcorp.com/mysql-testing:latest
	tox

itest:
	# TODO(DATAPIPE-134|justinc) This should run real integration tests
	true

clean:
	rm -rf docs/build
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
