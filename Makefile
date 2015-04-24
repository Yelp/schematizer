.PHONY: all production test itest docs clean

all: production

production:
	@true

docs:
	tox -e docs

test:
	docker pull docker-dev.yelpcorp.com/mysql-testing:latest
	tox -e py

itest:
	tox -e acceptance

clean:
	rm -rf docs/build
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
