.PHONY: fmt
fmt:
	hatch run fmt

.PHONY: chk
chk:
	hatch run chk

.PHONY: test
test: chk
	hatch run test

.PHONY: test-all
test-all: chk
	hatch -e test run test

.PHONY: test-sqla
test-sqla:
	hatch run test-sqla

.PHONY: test-sqla-all
test-sqla-all:
	hatch -e test run test-sqla

.PHONY: lock
lock:
	rm -rf ./requirements/
	hatch env run -- python --version
	hatch env run --env test -- python --version

.PHONY: upgrade-lock
upgrade-lock:
	rm -rf ./requirements/
	PIP_COMPILE_UPGRADE=1 hatch env run -- python --version
	PIP_COMPILE_UPGRADE=1 hatch env run --env test -- python --version
