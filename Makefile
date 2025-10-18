RUFF_VERSION := 0.9.1
TOX_VERSION := 4.23.2

.PHONY: fmt
fmt:
	# TODO: https://github.com/astral-sh/uv/issues/5903
	uvx ruff check --select I --fix .
	uvx ruff format .

.PHONY: chk
chk:
	uvx ruff check .
	uvx ruff format --check .
	uv run mypy .

.PHONY: test
test: chk
	uv run pytest -n 8 --cov pyathena --cov-report html --cov-report term tests/pyathena/

.PHONY: test-sqla
test-sqla:
	uv run pytest -n 8 --cov pyathena --cov-report html --cov-report term tests/sqlalchemy/

.PHONY: tox
tox:
	uvx tox run

.PHONY: docs
docs:
	uv build
	uv run sphinx-multiversion docs docs/_build/html
	echo '<meta http-equiv="refresh" content="0; url=./master/index.html">' > docs/_build/html/index.html
	touch docs/_build/html/.nojekyll

.PHONY: tool
tool:
	uv tool install ruff@$(RUFF_VERSION)
	uv tool install tox@$(TOX_VERSION) --with tox-uv --with tox-gh-actions
