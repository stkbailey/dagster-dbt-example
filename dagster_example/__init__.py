import pathlib
import json

from dagster import (
    Definitions,
    DagsterEvent,
    asset,
    OpExecutionContext,
    op,
    job,
    RunConfig,
    AssetKey,
    BindResourcesToJobs,
    Output,
    AssetMaterialization,
    Config,
    define_asset_job,
)
from dagster_dbt import (
    DbtCliResource,
    DagsterDbtTranslator,
    DagsterDbtCliHandledRuntimeError,
    DagsterDbtCliRuntimeError,
    dbt_assets,
    DbtManifestAssetSelection,
    build_dbt_asset_selection,
    get_asset_key_for_model,
)
from typing import Mapping, Any, List, Dict


class CustomDbtTranslator(DagsterDbtTranslator):
    @classmethod
    def get_asset_key(cls, dbt_resource_props) -> AssetKey:
        db = dbt_resource_props.get("database", "default").lower()
        schema = dbt_resource_props.get("schema", "public").lower()
        name = dbt_resource_props["name"].lower()
        return AssetKey(["snowflake", db, schema, name])

    @classmethod
    def get_group_name(cls, dbt_resource_props) -> str:
        return dbt_resource_props["resource_type"]
        # if len(dbt_resource_props["fqn"]) > 2:
        #     return f"{dbt_resource_props['fqn'][1]}__{dbt_resource_props['fqn'][2]}"
        # return f"{dbt_resource_props['fqn'][0]}"


manifest_path = pathlib.Path("dbt_warehouse/target/manifest.json")
DBT_MANIFEST = json.loads(manifest_path.read_text())


def prepare_dbt_args(command: str, args: str, vars: dict):
    cli_args = [command]
    if args:
        cli_args.extend(args.strip().split(" "))
    if vars:
        # have to remove whitespace from json string :(
        sanitized = json.dumps(vars, separators=(",", ":"))
        cli_args.extend(["--vars", sanitized])
    return cli_args

def yield_asset_materialization(dagster_event):
    if isinstance(dagster_event, Output):
        node_id = dagster_event.metadata["unique_id"].value
        # if not node_id.startswith("test"):
        manifest_node_info = DBT_MANIFEST["nodes"][node_id]
        yield AssetMaterialization(
            asset_key=CustomDbtTranslator.get_asset_key(manifest_node_info),
            metadata=dagster_event.metadata,
        )


class DbtAdHocOpConfig(Config):
    """
    Configuration for dbt jobs that run through the dagster-dbt CLI.

    Args:
        command: The dbt command to run, such as `build`, `run`, `test`, or `seed`.
        args: Any additional command line arguments to pass to the job. To pass in `vars`, use the vars argument.
        fail_job_on_error: Whether the run itself should fail if a single test fails
        vars: A dictionary of variables to pass to the dbt command. These will be passed as `--vars` arguments.
    """

    command: str = "run"
    args: str = None
    fail_job_on_error: bool = True
    vars: Dict[str, str] = {}


@op
def dbt_ad_hoc_cli_op(
    context: OpExecutionContext, config: DbtAdHocOpConfig, dbt: DbtCliResource
):
    try:
        # prepare the args
        args = prepare_dbt_args(config.command, config.args, config.vars)
        context.log.info("Received args: " + str(args))

        # run the command
        dbt_task = dbt.cli(args, manifest=DBT_MANIFEST, context=context)
        for dagster_event in dbt_task.stream():
            yield_asset_materialization(dagster_event)

        context.log.info("yay")

        # handle the results
        run_success = dbt_task.is_successful()

    except DagsterDbtCliHandledRuntimeError as e:
        context.log.exception("Error encountered during dbt run.")
        run_success = False

    except DagsterDbtCliRuntimeError as e:
        context.log.exception("Error encountered during dbt run.")
        run_success = False

    # do something with run results
    try:
        run_results = dbt_task.get_artifact("run_results.json")
        context.log.info("Creating Slack alerts from result failures")
        # post_slack_alerts(run_results, context)
    except FileNotFoundError:
        context.log.info("No run results found.")
        run_results = None

    # succeed or fail the run
    if not run_success and config.fail_job_on_error:
        raise Exception("dbt run was unsuccessful.")

    yield Output(value=run_results)


@job
def dbt_ad_hoc_cli_job():
    dbt_ad_hoc_cli_op()


@dbt_assets(manifest=DBT_MANIFEST, dagster_dbt_translator=CustomDbtTranslator)
def default_dbt_project_models(
    context: OpExecutionContext,
    dbt: DbtCliResource,
):
    try:
        # prepare the args
        args = [
            "build",
            "--resource-type",
            "model",
            "--resource-type",
            "seed",
            "--resource-type",
            "snapshot",
        ]
        context.log.info("Received args: " + str(args))

        # run the command
        dbt_task = dbt.cli(args, manifest=DBT_MANIFEST, context=context)
        yield from dbt_task.stream()

        run_success = dbt_task.is_successful()

    except DagsterDbtCliHandledRuntimeError as e:
        context.log.exception("Error encountered during dbt run.")
        run_success = False

    except DagsterDbtCliRuntimeError as e:
        context.log.exception("Error encountered during dbt run.")
        run_success = False

    # do something with run results
    try:
        run_results = dbt_task.get_artifact("run_results.json")
        context.log.info("Creating Slack alerts from result failures")
        # post_slack_alerts(run_results, context)
    except FileNotFoundError:
        context.log.info("No run results found.")

    # succeed or fail the run
    if not run_success:
        raise Exception("dbt run was unsuccessful.")



defs = Definitions(
    assets=[default_dbt_project_models],
    jobs=BindResourcesToJobs([dbt_ad_hoc_cli_job]),
    resources={
        "dbt": DbtCliResource(project_dir="dbt_warehouse"),
    },
)
