.PHONY: fmt
fmt:
	poetry run isort .
	poetry run black .

.PHONY: chk
chk:
	# https://github.com/PyCQA/flake8/issues/234
	poetry run flake8 --max-line-length 100 --exclude .poetry,.tox,.tmp .
	poetry run isort -c .
	poetry run black --check --diff .
	poetry run mypy .
