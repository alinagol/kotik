#!/usr/bin/env bash
set -e

pip install black==21.6b0
pip install pylint==2.8.3
pip install mypy==0.902
pip install isort==5.8.0
pip install types-requests==0.1.9

black --line-length=79 --check --diff api asyncworker
isort --check-only -skip-gitignore --diff api asyncworker
mypy --ignore-missing-imports api asyncworker
pylint --rcfile=.pylintrc api asyncworker
