.PHONY: install
install:
	pip3 install -r . requirements.txt

.PHONY: install_lint_requirements
install_lint_requirements:
	pip3 install black==21.11b1
	pip3 install pylint==2.12.2
	pip3 install mypy==0.920
	pip3 install isort==5.10.1

.PHONY: lint
lint: install_lint_requirements
	black --line-length=79 --check --diff api asyncworker
	isort --check-only --skip-gitignore --diff .
	pylint api asyncworker
	mypy --ignore-missing-imports api asyncworker

.PHONY: format
format: install_lint_requirements
	black --line-length=79 api asyncworker
	isort --skip-gitignore .
