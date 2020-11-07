.PHONY: fmt
fmt:
	poetry run isort .
	poetry run black .

.PHONY: chk
chk:
	poetry run isort -c .
	poetry run black --check --diff .
