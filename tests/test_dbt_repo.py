from dagster import RunConfig, materialize
from dagster_example import defs, dbt_warehouse_assets


AD_HOC_JOB_NAME = "dbt_ad_hoc_cli_job"
AD_HOC_OP_NAME = "dbt_ad_hoc_cli_op"


def test_definitions():
    assert len(defs.get_all_job_defs()) > 0


def test_dbt_compile_job():
    # given
    job = defs.get_job_def(AD_HOC_JOB_NAME)
    cfg = RunConfig(
        ops={
            AD_HOC_OP_NAME: {
                "config": {
                    "command": "compile",
                    "args": "--models group_1",
                }
            }
        }
    )

    # when
    result = job.execute_in_process(run_config=cfg)

    # then
    assert result.success


def test_dbt_seed_job():
    # given
    job = defs.get_job_def(AD_HOC_JOB_NAME)
    cfg = RunConfig(
        ops={
            AD_HOC_OP_NAME: {
                "config": {
                    "command": "seed",
                }
            }
        }
    )

    # when
    result = job.execute_in_process(run_config=cfg)

    # then
    assert result.success


def test_dbt_test_job():
    # given
    job = defs.get_job_def(AD_HOC_JOB_NAME)
    cfg = RunConfig(
        ops={
            AD_HOC_OP_NAME: {
                "config": {
                    "command": "test",
                    "fail_job_on_error": False,
                }
            }
        }
    )

    # when
    result = job.execute_in_process(run_config=cfg)

    # then
    assert result.success


def test_materialize_models():
    # given
    asset_resources = defs.get_repository_def().get_top_level_resources()

    # when
    result = materialize([dbt_warehouse_assets], resources=asset_resources)

    # then
    assert result.success
