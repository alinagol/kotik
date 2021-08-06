#!/usr/bin/env bash
set -e

black --line-length=79 api asyncworker
isort -skip-gitignore api asyncworker
mypy --ignore-missing-imports api asyncworker
