# lots of imports!
import pathlib
import json

from dagster import (
    Definitions,
    OpExecutionContext,
    op,
    job,
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
    build_dbt_asset_selection,
)
from typing import Mapping, Any, List, Dict


# this "translator" object is used to map dbt nodes/metadata to dagster asset keys/metadata
class CustomDbtTranslator(DagsterDbtTranslator):
    @classmethod
    def get_asset_key(cls, dbt_resource_props) -> AssetKey:
        "Use a database/schema/table designator."
        db = dbt_resource_props.get("database", "unset").lower()
        schema = dbt_resource_props.get("schema", "unset").lower()
        name = dbt_resource_props["name"].lower()
        return AssetKey([db, schema, name])

    @classmethod
    def get_group_name(cls, dbt_resource_props) -> str:
        "Create asset groups based on resource type."
        return dbt_resource_props["resource_type"]


# you are expected to build the manifest prior to loading this file
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


class DbtAdHocOpConfig(Config):
    "Configuration for dbt jobs that are run as ops/jobs."
    command: str = (
        "run"  # The dbt command to run, such as `build`, `run`, `test`, or `seed`.
    )
    args: str = None  # Any additional command line arguments to pass to the job. To pass in `vars`, use the vars argument.
    fail_job_on_error: bool = True  # Whether the run itself should fail if the task fails. (e.g. could be used to ignore if a test fails)
    vars: Dict[
        str, str
    ] = (
        {}
    )  # A dictionary of variables to pass to the dbt command. These will be passed as `--vars` arguments.


class DbtAdHocAssetConfig(Config):
    "Configuration for dbt jobs that are run as asset materializations."
    command: str = "build"
    args: str = "--resource-type model --resource-type seed --resource-type snapshot"
    vars: Dict[str, str] = {"deployment": "stage"}
    fail_job_on_error: bool = True


@op
def dbt_ad_hoc_cli_op(
    context: OpExecutionContext, config: DbtAdHocOpConfig, dbt: DbtCliResource
):
    "This op runs an ad hoc dbt comman via the CLI."
    try:
        args = prepare_dbt_args(config.command, config.args, config.vars)
        dbt_task = dbt.cli(args, manifest=DBT_MANIFEST, context=context)
        for dagster_event in dbt_task.stream():
            # todo: remove this once you can request this via `.stream(yield_asset_materialization=true)`
            if isinstance(dagster_event, Output):
                node_id = dagster_event.metadata["unique_id"].value
                manifest_node_info = DBT_MANIFEST["nodes"][node_id]
                yield AssetMaterialization(
                    asset_key=CustomDbtTranslator.get_asset_key(manifest_node_info),
                    metadata=dagster_event.metadata,
                )

        run_success = dbt_task.is_successful()

    except DagsterDbtCliRuntimeError as e:
        context.log.exception("Error encountered during dbt run.")
        run_success = False

    # do something with run results or other artifacts
    try:
        run_results = dbt_task.get_artifact("run_results.json")
        context.log.info(f"Did {len(run_results['results'])} things.")
    except FileNotFoundError:
        context.log.info("No run results found.")

    # succeed or fail the run
    if not run_success and config.fail_job_on_error:
        raise Exception("dbt run was unsuccessful.")

    # dbt op needs to return an output, but assets do not
    yield Output(None)


@job
def dbt_ad_hoc_cli_job():
    dbt_ad_hoc_cli_op()


@dbt_assets(manifest=DBT_MANIFEST, dagster_dbt_translator=CustomDbtTranslator)
def dbt_warehouse_assets(
    context: OpExecutionContext,
    config: DbtAdHocAssetConfig,
    dbt: DbtCliResource,
):
    "This op runs a dbt command vis a vis the Dagster asset materialization API."
    try:
        args = prepare_dbt_args(config.command, config.args, config.vars)
        dbt_task = dbt.cli(args, manifest=DBT_MANIFEST, context=context)
        yield from dbt_task.stream()

        run_success = dbt_task.is_successful()

    except DagsterDbtCliRuntimeError as e:
        context.log.exception("Error encountered during dbt run.")
        run_success = False

    # do something with run results or other artifacts
    try:
        run_results = dbt_task.get_artifact("run_results.json")
        context.log.info(f"Did {len(run_results['results'])} things.")
    except FileNotFoundError:
        context.log.info("No run results found.")

    # succeed or fail the run
    if not run_success and config.fail_job_on_error:
        raise Exception("dbt run was unsuccessful.")


all_dbt_assets_job = define_asset_job(
    name="all_dbt_assets_job",
    selection=[dbt_warehouse_assets],
)

# todo: add these asset jobs once we have a way to run add CustomDbtTranslator
# group_1_asset_job = define_asset_job(
#     name="bespoke_asset_job",
#     selection=build_dbt_asset_selection(
#         dbt_assets=[dbt_warehouse_assets],
#         dbt_select="fqn:*",
#     ),
# )

# group_2_asset_job = define_asset_job(
#     name="bespoke_asset_job",
#     selection=build_dbt_asset_selection(
#         dbt_assets=[dbt_warehouse_assets],
#         dbt_select="fqn:*",
#     ),
# )

job_list = [
    dbt_ad_hoc_cli_job,
    all_dbt_assets_job,
    # group_1_asset_job,
    # group_2_asset_job
]

defs = Definitions(
    assets=[dbt_warehouse_assets],
    jobs=BindResourcesToJobs(job_list),
    resources={
        "dbt": DbtCliResource(project_dir="dbt_warehouse"),
    },
)
