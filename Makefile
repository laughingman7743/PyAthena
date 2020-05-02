.PHONY: fmt
fmt:
	pipenv run isort -rc .
	pipenv run black .

.PHONY: chk
chk:
	pipenv run isort -c -rc .	
	pipenv run black --check --diff .
