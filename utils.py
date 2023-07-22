



    """
    This op executes a `dbt <command> <args>` CLI command. It requires
    the use of a dbt resource, which can be set to execute this command
    through the CLI (using the :py:class:`~dagster_dbt.dbt_cli_resource`).

    Examples:

    .. code-block:: python

        from dagster import job
        from dagster_dbt import dbt_cli_resource
        from .ops import custom_dbt_build_op

        @job(resource_defs={{"dbt": dbt_cli_resource}})
        def my_dbt_cli_job():
            custom_dbt_build_op()
    """

    # run the custom command
    try:
        if config.vars:
            # have to remove whitespace from json string :(
            sanitized = json.dumps(config["vars"], separators=(",", ":"))
            cmd = f"""{config.command} --vars {sanitized} {config.args}"""
        else:
            cmd = f"""{config.command} {config.args}"""
        context.log.info("Running the command: " + cmd)

        # run the command
        cli_task = dbt.cli(
            command=cmd.split(" "),
            manifest=dbt_manifest,
            context=context,
            dagster_dbt_translator=CustomDagsterDbtTranslator,
        )
        yield cli_task.stream()

        run_success = True

    except DagsterDbtCliHandledRuntimeError as e:
        context.log.exception("Error encountered during dbt run.")
        run_success = False

    except DagsterDbtCliOutputsNotFoundError as e:
        context.log.exception("The dbt function did not return expected results.")
        run_success = False

    try:

        context.log.info("Logging events from dbt.")
        manifest = read_json_file(f"{dbt.project_dir}/target/manifest.json")
        run_results = read_json_file(f"{dbt.project_dir}/target/run_results.json")

        # parses the run_results and manifest and posts errors to Slack
        context.log.info("Creating Slack alerts from result failures")
        slack_client = slack.get_client()
        job_url = "https://whatnot.dagster.cloud/"
        job_url += f"{CURRENT_DEPLOYMENT}/runs/{context.run_id}"
        alerts = create_slack_alerts_from_dbt_results(
            run_results=run_results, manifest=manifest, job_url=job_url
        )
        for alert in alerts:
            context.log.info(
                "Posting failure message for %s to %s: %s",
                alert.name,
                alert.channel,
                alert.message_block,
            )
            slack_client.chat_postMessage(
                channel=alert.channel, blocks=[alert.message_block]
            )

    except DagsterDbtCliOutputsNotFoundError as e:
        context.log.exception(
            "The dbt function did not return expected results. Skipping alerts."
        )
        run_results = {}

    except FileNotFoundError as e:
        context.log.exception("Run results not found. Skipping alerts.")
        run_results = {}

    except SlackApiError:
        context.log.exception("Slack API error: Could not post message %s")

    # succeed or fail the run
    if not run_success and config["fail_job_on_error"]:
        raise Exception("dbt run was unsuccessful.")

    return run_results
