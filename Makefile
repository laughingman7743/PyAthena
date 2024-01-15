.PHONY: fmt
fmt:
	pdm run ruff check --select I --fix .
	pdm run ruff format .

.PHONY: chk
chk:
	pdm run ruff check .
	pdm run ruff format --check .
	pdm run mypy .


.PHONY: test
test: chk
	pdm run pytest -n 8 --cov pyathena --cov-report html --cov-report term tests/pyathena/

.PHONY: test-sqla
test-sqla:
	pdm run pytest -n 8 --cov pyathena --cov-report html --cov-report term tests/sqlalchemy/

.PHONY: tox
tox:
	pdm run tox
