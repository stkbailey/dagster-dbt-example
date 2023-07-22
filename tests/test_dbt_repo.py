from dagster import RunConfig
from dagster_example import defs


AD_HOC_JOB_NAME = "dbt_ad_hoc_cli_job"
AD_HOC_OP_NAME = "dbt_ad_hoc_cli_op"


def test_definitions():
    assert len(defs.get_all_job_defs()) > 0


def test_dbt_compile_job():
    job = defs.get_job_def(AD_HOC_JOB_NAME)
    result = job.execute_in_process(
        run_config=RunConfig(
            ops={
                AD_HOC_OP_NAME: {
                    "config": {
                        "command": "compile",
                        "args": "--models group_1",
                    }
                }
            }
        )
    )

    assert result.success


def test_dbt_seed_job():
    job = defs.get_job_def(AD_HOC_JOB_NAME)
    result = job.execute_in_process(
        run_config=RunConfig(
            ops={
                AD_HOC_OP_NAME: {
                    "config": {
                        "command": "seed",
                    }
                }
            }
        )
    )

    assert result.success


def test_dbt_test_job():
    job = defs.get_job_def(AD_HOC_JOB_NAME)
    result = job.execute_in_process(
        run_config=RunConfig(
            ops={
                AD_HOC_OP_NAME: {
                    "config": {
                        "command": "test",
                        "fail_job_on_error": False,
                    }
                }
            }
        )
    )

    assert result.success
