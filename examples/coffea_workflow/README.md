# Simple accumulator workflow

The basic end-to-end example: a MET analysis over two CMS Open Data muon datasets, wired into a fileset → analysis → plotting DAG. Results are merged as plain coffea accumulators — no histogram server involved.

- [analysis.py](analysis.py) — `get_fileset`, the `Processor` (a plain `coffea.processor.ProcessorABC` subclass), `plot_results`, plus a custom filtering helper
- [workflow.ipynb](workflow.ipynb) — builds the DAG and runs it

Beyond the basics, the notebook demonstrates:

- **Declarative `Analysis` step** — `Step(step_type=Analysis, processor="analysis:Processor", runner_params={...})`. coffea-workflow builds and drives the `coffea.processor.Runner` itself (injecting the executor, forcing `use_result_type=True`); you only write the `Processor`. No `run_analysis(fileset, executor)` wrapper needed.
- **`builder_params`** — passing extra keyword arguments to a builder from the `Step` definition
- **`CustomArtifact`** — defining your own intermediate step (`custom_function_remove_last_file`) alongside the predefined artifact types
- **String builder/processor references** — `"analysis:plot_results"` / `"analysis:Processor"` instead of importing the callable/class
- **Facility switching** — `RunConfig` variants for local `FuturesExecutor`, coffea-casa `FuturesExecutor`, and coffea-casa `DaskExecutor` with `worker_packages`/`worker_files`

If your analysis needs custom orchestration that a plain `Processor` + `runner_params` can't express — e.g. picking the executor at runtime, or constructing the processor from a live connection — use the escape hatch instead: `Step(builder="module:function")`, where your function builds and drives its own `Runner`. See [../agc_ttbar/](../agc_ttbar/) (dynamic executor selection) or [../coffea_workflow_histserv/](../coffea_workflow_histserv/) (processor built from a live histserv connection) for that style.

For the same workflow streaming histograms to a histserv server instead, see [../coffea_workflow_histserv/](../coffea_workflow_histserv/).
