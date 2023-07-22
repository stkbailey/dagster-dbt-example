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

compile:
	.venv/bin/dbt compile --project-dir dbt_warehouse