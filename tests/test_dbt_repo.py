from dagster import RunConfig
from dagster_example import defs, dbt_test_job


def test_definitions():
    assert len(defs.get_all_job_defs()) > 0


def test_dbt_compile_job():
    job = defs.get_job_def("dbt_test_job")
    result = job.execute_in_process(
        run_config=RunConfig(
            ops={
                "dbt_test_op": {
                    "config": {
                        "command": "compile",
                        "args": "--models applications",
                    }
                }
            }
        )
    )

    assert result.success


def test_dbt_seed_job():
    job = defs.get_job_def("dbt_test_job")
    result = job.execute_in_process(
        run_config=RunConfig(
            ops={
                "dbt_test_op": {
                    "config": {
                        "command": "seed",
                    }
                }
            }
        )
    )

    assert result.success


def test_dbt_test_job():
    job = defs.get_job_def("dbt_test_job")
    result = job.execute_in_process(
        run_config=RunConfig(
            ops={
                "dbt_test_op": {
                    "config": {
                        "command": "test",
                    }
                }
            }
        )
    )

    assert result.success
