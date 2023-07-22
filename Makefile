setup:
	python -m venv .venv
	.venv/bin/pip install poetry
	.venv/bin/poetry install --with dev

update:
	.venv/bin/poetry update

run:
	.venv/bin/dagster dev --module-name dagster_example

test:
	.venv/bin/pytest tests

format:
	.venv/bin/black dagster_example tests

clean:
	.venv/bin/dbt clean --project-dir dbt_warehouse

# the manifest must be compiled from within the dbt project
# for seed file locations to resolve properly
compile: clean
	cd dbt_warehouse && ../.venv/bin/dbt compile
