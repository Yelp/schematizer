.PHONY: all production test docs clean

all: production

production:
	@true

docs:
	tox -e docs

test:
	tox

clean:
	rm -rf docs/build
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
