PYTHON ?= python
COMPOSE = docker compose --env-file lab/.env -f lab/compose.yaml

.PHONY: setup lint test test-unit test-property test-contract test-planner build verify-wheel lab-env lab-cert lab-up lab-seed lab-run lab-test lab-reset lab-down e2e

setup:
	$(PYTHON) -m pip install -e '.[test,azure]'

lint:
	ruff check src tests

test:
	PYTHONPATH=src $(PYTHON) -m pytest -q tests/architecture tests/unit tests/property tests/conformance tests/integration

test-unit:
	PYTHONPATH=src $(PYTHON) -m pytest -q tests/unit tests/architecture

test-property:
	PYTHONPATH=src $(PYTHON) -m pytest -q tests/property

test-contract:
	PYTHONPATH=src $(PYTHON) -m pytest -q tests/conformance/test_contract.py

test-planner:
	PYTHONPATH=src $(PYTHON) -m pytest -q tests/unit/test_planner.py tests/property/test_partition_coverage.py

build:
	rm -f dist/landfall-*.whl
	$(PYTHON) -m build --wheel

verify-wheel: build
	$(PYTHON) -m venv --clear .venv-wheel
	env -u PYTHONPATH .venv-wheel/bin/pip install dist/landfall-*.whl
	env -u PYTHONPATH .venv-wheel/bin/landfall version
	env -u PYTHONPATH .venv-wheel/bin/landfall validate-contract --contract contracts/examples/customer.incremental.yaml

lab-env:
	@if [ ! -f lab/.env ]; then openssl rand -base64 32 | awk '{print "LANDFALL_LAB_STORAGE_KEY=" $$0}' > lab/.env; fi

lab-cert:
	mkdir -p lab/certs
	openssl req -x509 -newkey rsa:2048 -sha256 -days 365 -nodes -keyout lab/certs/emulator.key -out lab/certs/emulator.crt -subj '/CN=keyvault' -addext 'subjectAltName=DNS:keyvault,DNS:localhost'
	openssl pkcs12 -export -out lab/certs/emulator.pfx -inkey lab/certs/emulator.key -in lab/certs/emulator.crt -passout pass:emulator

lab-up: lab-env lab-cert
	$(COMPOSE) up -d sqlserver floci-az keyvault

lab-seed: build lab-env
	$(COMPOSE) run --build --rm runner seed

lab-run: lab-env
	$(COMPOSE) run --rm runner run

lab-test: lab-env
	$(COMPOSE) run --rm runner verify

lab-reset: lab-env
	$(COMPOSE) down -v

lab-down: lab-env
	$(COMPOSE) down

e2e: build lab-env lab-cert
	$(COMPOSE) down -v --remove-orphans
	$(COMPOSE) up -d sqlserver floci-az keyvault
	@trap '$(COMPOSE) down -v --remove-orphans' EXIT; $(COMPOSE) run --build --rm runner e2e
