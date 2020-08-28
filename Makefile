.PHONY: fmt
fmt:
	pipenv run isort .
	pipenv run black .

.PHONY: chk
chk:
	pipenv run isort -c .
	pipenv run black --check --diff .
