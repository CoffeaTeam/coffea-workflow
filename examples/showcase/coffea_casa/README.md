# CoffeaCasa + Dask

Run the showcase analysis on [coffea-casa](https://coffea-casa.readthedocs.io), which provides a pre-configured Dask cluster at `tls://localhost:8786`.

Describes examples when no setup is needed and `CoffeaCasaFactory` connects to the existing cluster. And also describes the cases where the workers are required too install additional packages.

## Options shown

- **Sequential** (`workflow_coffea_casa.ipynb`): fileset chunks are processed one after another using the split strategy. Simple, cache-friendly.
- **Parallel** *(TODO)*: use `client.submit` + `IterativeExecutor` to dispatch multiple fileset subsets to the Dask cluster simultaneously, bypassing coffea-workflow's
sequential chunk loop. *How to learn Dask to see one batch as one job? Bigger coffea-chunks?*
- discuss the use case where you want to install some other packages on Dask workers and that
 some workers result in not having the environment updated while others do

## Current problems with DaskExecutor usage

Using `DaskExecutor` with `coffea-workflow`'s split strategy is currently unavailable on CoffeaCasa due to a hard dependency on the exact coffea version installed in the worker image.

**Issues:**

1. `coffea-workflow`'s split strategy uses `coffea.dataset_tools.splitting.split_fileset`, which wasn't introduced in the earlier coffea version. coffea-casa doesn't support this one yet.
   
2. The natural fix is to install a consistent coffea version on workers at runtime via `worker_packages`. However, any runtime package installation requires restarting workers so the new version is actually loaded.

3. Worker restarts breaks Dask scheduler. `FutureCancelledError: scheduler-restart`.

4. Without restart not all the workers use the newly PipInstalled version of coffea and some chunks break and return the error that `use_result_type` field wasn't found in the coffea Runner.

5. The alternative — installing with `client.run()` then calling `client.restart()` explicitly. But `client.restart()` is a scheduler-level restart that also cancels any futures submitted after the install, making it impossible to pipeline setup and execution.

**In short:** everything about running `coffea-workflow` with split strategy on CoffeaCasa Dask workers depends on having a single consistent coffea version across the notebook server, the scheduler, and every worker pod — something that cannot be reliably achieved without controlling the worker container image.

## What works

As for now, before coffea-casa has the newest version of coffea, it's recommended to use lxplus + dask.
`FuturesExecutor` runs locally on the notebook server using Python multiprocessing. 