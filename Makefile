PKGNAME=pysixdesk

all: install

install:
	pip install .

install-dev:
	pip install -r requirements-dev.txt
	pip install -e .

clean:
	python setup.py clean --all

dependencies:
	pip install -Ur requirements.txt

test: test-all

test-all:
	py.test --junitxml=./reports/junit.xml -o junit_suite_name=$(PKGNAME) tests

test-sqlite:
	py.test --junitxml=./reports/junit.xml -o junit_suite_name=$(PKGNAME) tests/integration/sqlite

test-mysql:
	py.test --junitxml=./reports/junit.xml -o junit_suite_name=$(PKGNAME) tests/integration/mysql

test-integration:
	py.test --junitxml=./reports/junit.xml -o junit_suite_name=$(PKGNAME) tests/integration/

test-unit:
	py.test --junitxml=./reports/junit.xml -o junit_suite_name=$(PKGNAME) tests/unit

test-cov-unit:
	py.test --cov ./tests/unit --cov-report term-missing --cov-report xml:reports/coverage.xml --cov-report html:reports/coverage tests/unit

test-cov-integration:
	py.test --cov ./tests/integration --cov-report term-missing --cov-report xml:reports/coverage.xml --cov-report html:reports/coverage tests/integration

.PHONY: all clean install install-dev dependencies test test-all test-sqlite test-mysql test-integration test-unit test-cov
