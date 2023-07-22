# example project w/ dagster-dbt

**N.B.** By the time you probably read this, this repository will probably be out of date and/or bad practice!!

It took me a bit to figure out a preferred way of running dbt via Dagster, so this
repository is both an encapsulation of the way I like to think about the problem,
as well as sandbox implementation of new dagster-dbt APIs introduced in v1.4.0.

All Dagster code is in `dagster_example/__init__.py`. The dbt project is in `dbt_warehouse`.

## Goals

Here's a few of the goals.

- I want a Dagster AssetMaterialization event streamed, every time a dbt-managed seed/model/snapshot is run.
- I want to run non-traditional asset jobs, like `dbt test`, `dbt run-operation`, and `dbt source freshness`.
- I want to run custom jobs from the launchpad with different args, like `--full-refresh` or `--vars {start_time:2023-07-01}`.
- I want to create preconfigured / scheduled jobs that have custom config, but still yield AssetMaterializations.
- I want to do custom things with post-run artifacts, like send Slack messages based on the `run_results.json`

## Approach

The approach shown here has two arms:

1. An `ad_hoc_cli_op`/`job` that runs a command line argument based on config.
2. An `@dbt_assets` function that generates the assets.

The logic for the two arms should be almost identical, but there are slight nuances.
Note that while it's technically possible to _just_ use the @dbt_assets functionality to run even tests,
it's conceptually useful to have a non-asset-based "job" that is roughly equivalent
to running an arg from the command line. (In particular, it's easier for less Dagster-savvy users.)

## Trying it out

You should be able to run:

```
make setup
make compile
make run
```

This will launch the Dagster server. If you don't have dbt configured, you might
have a mess on your hands, but I'm not going to help you with that.
