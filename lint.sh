#!/usr/bin/env bash
set -e

black --line-length=79 --check --diff api asyncworker
isort --check-only -skip-gitignore --diff api asyncworker
mypy --ignore-missing-imports api asyncworker
pylint --rcfile=.pylintrc api asyncworker
