# coffea-workflow

A workflow manager and HEP-specific extension for [coffea](https://github.com/scikit-hep/coffea) analyses. It does not replace existing workflow managers (Snakemake, LAW, …) — instead it focuses on three things coffea alone does not provide out of the box:

- **Partial results** — split your fileset into independently cached chunks; if some fail you keep the rest, and only the failed chunks are retried on the next run
- **Facility factories** — one-line switching between local execution, [coffea-casa](https://coffea-casa.readthedocs.io), and CERN lxplus (HTCondor) without changing your analysis code
- **Execution control** — choose between sequential and parallel chunk submission, tune executor type and worker count per facility

Your analysis code stays unchanged and fully separate from the execution logic. The only shift in thinking is structural: instead of one monolithic script, you organise the code around the natural stages of a HEP pipeline — fileset discovery, running the processor, plotting, and so on — and hand each stage to the workflow as a step. How you write each function is up to you.

---

## Why coffea-workflow
 
Some HEP analyses share 
1) a similar structure (multiple sequential steps: discovering input files, splitting them into manageable chunks, running a coffea processor over each chunk, merging partial results, and producing final plots),
2) as well as similar practices that are manually re-implemented from scratch each time (splitting the fileset to test on a smaller subset of files, implementing local caching of partial results, and so on). 

Without a pre-defined workflow layer, coffea users tend to write ad-hoc scripts that are sometimes difficult to reproduce, cannot skip already-completed work, mix workflow execution logic with analysis logic, and lose partial progress when, for example, a remote file server is temporarily unreachable.
 
`coffea-workflow` addresses this by:
 
- Defining each stage as a typed, hashable **Artifact** with a deterministic identity derived from its inputs.
- Storing every produced artifact in a **content-addressable cache** (`.cache/`), so any step whose inputs have not changed is loaded from disk on the next run.
- Providing **chunk-level fault tolerance**: if 4 out of 5 chunks succeed and one fails (e.g. a broken XRootD endpoint), the successful chunks are preserved and only the failed chunk is retried on the next run.
- Keeping **framework logic cleanly separated** from analysis code — no decorators on your functions, no YAML.

---

## Installation

`coffea-workflow` requires coffea `2026.7.0` or newer, which exposes `use_result_type=True` on `processor.Runner`, enabling the `Ok`/`Err` result-type pattern used by the fault-tolerance mechanism. It is installed automatically as a dependency.

```bash
pip install coffea-workflow
```

### Optional — histserv

```bash
pip install histserv
```
---

## Quick Start

Separate your analysis into stand-alone functions, one per workflow stage:

```python
# analysis.py

def get_fileset():
    return {
        "SingleMuon_2018A": {
            "files": {"root://cmsxrootd.fnal.gov//store/...": "Events"},
        }
    }

def run_analysis(fileset, executor):
    # your existing coffea processor call here — return Ok(output) or Err(exception)
    result = processor.Runner(executor=executor, ...)(fileset, ...)
    return result

def plot_results(analysis_output):
    ...
```

Then wire them together in a notebook or script:

```python
from coffea_workflow import Step, Workflow, Fileset, Analysis, Plotting, RunConfig, ExecutorConfig, run
from coffea_workflow import facilities
from analysis import get_fileset, run_analysis, plot_results

# 1. Define steps — map artifact types to your functions
step_fileset  = Step(name="Fileset",  step_type=Fileset,  builder=get_fileset,
                     output="fileset")
step_analysis = Step(name="Analysis", step_type=Analysis, builder=run_analysis,
                     input="fileset",  output="histograms")
step_plotting = Step(name="Plotting", step_type=Plotting, builder=plot_results,
                     input="histograms")

# 2. Build the DAG
workflow = Workflow()
workflow.add(step_fileset)
workflow.add(step_analysis, depends_on=[step_fileset])
workflow.add(step_plotting, depends_on=[step_analysis])

# 3. Configure and run
config = RunConfig(
    strategy="by_dataset",  # datasets are processed independently
    facility=facilities.coffea_casa,
    executor_config=ExecutorConfig(executor_type="DaskExecutor", workers=4),
    cache_dir=".cache",
)
run(workflow, config)
```

That is the whole API surface. `coffea-workflow` handles caching, splitting, fault-tolerance, and client setup for the environment automatically.

---

## Key Features

### Split Strategies

`coffea-workflow` breaks the fileset into independent *chunks* (subsets of the fileset) before running. Each chunk is a sub-fileset processed and cached on its own, so partial results are preserved even when some chunks fail.

> **Important distinction:** these are *workflow-level* chunks (sub-filesets), not coffea's internal 50k-event chunks. A single workflow chunk may still contain many coffea-level event batches.

| Strategy | Chunks | Best for |
|---|---|---|
| `strategy=None` (default) | 1 whole fileset | small tests, single-dataset runs |
| `strategy="by_dataset"` | 1 per dataset | multi-dataset runs, dataset-level fault isolation |
| `strategy=None, percentage=20` | 5 mixed across all datasets | quick sanity checks on a representative slice |
| `strategy="by_dataset", percentage=20` | 5 per dataset (15 total for 3 datasets) | large filesets, maximum fault tolerance |

**Smaller chunks preserve more work on failure** — only the failed chunk is retried, not the whole analysis. However, very small chunks add scheduling overhead on batch systems (more HTCondor job submissions). See [examples/showcase/split_strategy/](https://github.com/CoffeaTeam/coffea-workflow/tree/main/examples/showcase/split_strategy/) for a worked notebook of each strategy.

```python
# One chunk per dataset — if one dataset's storage fails, the others succeed
RunConfig(strategy="by_dataset")

# Split each dataset into 5 chunks of 20% each
RunConfig(strategy="by_dataset", percentage=20)

# Only run over specific datasets (e.g. for a quick test)
RunConfig(datasets=["SingleMuon_2018A"])
```

---

### Facility Factories

Switching execution environments is a one-line change in `RunConfig`. Your analysis code is untouched.

```python
from coffea_workflow import facilities

# Local — FuturesExecutor with N workers (default)
config = RunConfig(facility=facilities.local)

# coffea-casa — DaskExecutor connecting to the pre-configured Dask scheduler
config = RunConfig(facility=facilities.coffea_casa)

# CERN lxplus — HTCondor cluster, workers running inside an Apptainer image
config = RunConfig(facility=facilities.LxplusFactory(
    worker_image="~/worker.sif",
    queue="longlunch",
    workers=10,
))
```

Each factory also accepts an `ExecutorConfig` that overrides the default executor type:
```python
config = RunConfig(
    facility=facilities.local,
    executor_config=ExecutorConfig(executor_type="IterativeExecutor"),  # single-threaded
)
```

#### lxplus deployment

If you have no `worker.sif` yet, run your script locally first — `LxplusFactory` generates `worker.def` and `run_on_lxplus.sh` with exact build and run instructions. You can also generate the Apptainer definition file manually:

```python
from coffea_workflow.facilities import generate_apptainer_def
generate_apptainer_def(extra_packages=("correctionlib==2.1.0",))
```

See [examples/showcase/facilities/](https://github.com/CoffeaTeam/coffea-workflow/tree/main/examples/showcase/facilities/) for a full worked example.

---

### Sequential vs Parallel Chunk Execution

By default, `coffea-workflow` processes workflow chunks **sequentially**: one chunk is submitted to the executor, runs to completion, its result is cached, then the next chunk starts. This is the safer default because all N workers collaborate on a single chunk's event-level tasks.

**When to prefer sequential (default):**
- You have more workers than chunks — all workers collaborate per chunk and self-balance across its tasks
- Chunks have unequal file counts — avoids the slowest-chunk bottleneck that parallel dispatch creates

**When to consider parallel:**
- You have many more chunks than workers and they are roughly equal in size
- You want to minimise scheduler round-trip overhead on coffea-casa (Dask cluster is persistent)

```python
# Sequential (default) — one chunk at a time, all workers per chunk
config = RunConfig(
    facility=facilities.coffea_casa,
    executor_config=ExecutorConfig(executor_type="DaskExecutor"),
)

# Parallel — all chunks submitted simultaneously, one worker per chunk
config = RunConfig(
    facility=facilities.coffea_casa,
    executor_config=ExecutorConfig(executor_type="DaskExecutor", parallel_chunks=True),
)
```

A worked analysis of the trade-offs is in [examples/showcase/optimisation/](https://github.com/CoffeaTeam/coffea-workflow/tree/main/examples/showcase/optimisation/).

---
 
## Repository structure

```
coffea-workflow/
├── src/
│   └── coffea_workflow/
│       ├── __init__.py            # public API: Step, Workflow, run, RunConfig, Fileset, Analysis,
│       │                          #   Plotting, ExecutorConfig, facilities, detect_histserv_address
│       ├── artifacts.py           # Artifact classes (Fileset, Analysis, Plotting,
│       │                          #   Chunking, ChunkAnalysis, CustomArtifact)
│       ├── identity.py            # Deterministic hashing of an artifact's identity
│       ├── config.py              # RunConfig, ExecutorConfig, FacilityBase
│       ├── facilities.py          # LocalFactory, CoffeaCasaFactory, LxplusFactory
│       ├── producers.py           # @producer registry (artifact type -> producer fn)
│       ├── default_producers.py   # Built-in producers for each artifact type
│       ├── producers_utils.py     # Builder invocation, executor building, declarative-Runner helper
│       ├── deps.py                # Deps — materializes upstream artifacts on demand
│       ├── executor.py            # Cache lookup and materialization
│       ├── histserv_utils.py      # histserv address detection + auto reconnect/recreate
│       ├── render.py              # run() — topological sort + DAG execution
│       └── workflow.py            # Step dataclass, Workflow DAG container
├── examples/
│   ├── showcase/                  # Minimal MET analysis demonstrating all features
│   │   ├── split_strategy/        # One notebook per split strategy
│   │   ├── facilities/            # facility factories worked example
│   │   ├── coffea_casa/           # coffea-casa worked example
│   │   ├── lxplus/                # CERN lxplus (HTCondor) worked example
│   │   └── optimisation/          # Sequential vs parallel benchmarks (in progress)
│   ├── agc/                       # Full AGC ttbar analysis with coffea-workflow (+ histserv variant)
│   ├── coffea_workflow/           # Simple accumulator example (no histserv)
│   └── coffea_workflow_histserv/  # Same analysis with histserv backend
└── README.md
```
---
 
## Concepts

### Workflow & Step

A **`Workflow`** is a container for a directed acyclic graph. It holds **`Step`** objects and directed dependency edges.

```python
@dataclass(frozen=True)
class Step:
    name: str              # human-readable label; used as a cache-path component
    step_type: Type        # Fileset, Analysis, or Plotting
    builder: str | Callable  # pointer to your function
```

`builder` can be a callable or a `"module:attribute"` string (e.g. `"analysis:plot_results"`).

```python
workflow = Workflow()
workflow.add(step_fileset)
workflow.add(step_analysis, depends_on=[step_fileset])
workflow.add(step_plotting, depends_on=[step_analysis])
```

---

### The `Analysis` step: `processor=` (declarative) or `builder=` (function)

An `Analysis` step can be defined two ways. Choose based on **what the wrapper around your coffea `Runner` has to do** — *not* on how complex your `Processor` is. Declarative mode replaces only the Runner boilerplate, never `process()` itself, so a 500-line `Processor` can still be declarative.

**Declarative — you write only the `Processor`; the framework owns the `Runner`:**

```python
step_analysis = Step(
    name="Analysis", step_type=Analysis,
    processor="analysis:MyProcessor",              # a coffea ProcessorABC subclass ("module:Class" or the class)
    processor_params={"year": 2018},               # -> MyProcessor(**processor_params)
    runner_params={"schema": NanoAODSchema, "chunksize": 100_000, "skipbadfiles": True},
)
```

The framework builds `Runner(executor=<injected>, use_result_type=True, **runner_params)` and calls `runner(fileset, MyProcessor(**processor_params))`. `processor_params`/`runner_params` must be static, cache-stable values (classes are fine, e.g. `schema=NanoAODSchema`); they enter the artifact identity, so changing one correctly invalidates the cache. `use_result_type` and `executor` are framework-controlled and may not be set in `runner_params`.

**Escape hatch — you write a function that builds and runs the `Runner` yourself:**

```python
step_analysis = Step(name="Analysis", step_type=Analysis, builder="analysis:run_analysis")
```

```python
def run_analysis(fileset, executor):        # `config` is also injectable if declared
    run = processor.Runner(executor=executor, use_result_type=True, ...)
    return run(fileset, MyProcessor())
```

The function receives the framework-built `executor` (and the `RunConfig` as `config`, if it declares that parameter). Full control.

**When can you skip the function?** When your analysis is exactly: *build one standard `Runner` from static settings, run one `Processor` from static settings, return the result — nothing else.*

**You need the function when any of these is true:**

- **Logic around the run** — inspecting/short-circuiting the fileset, branching, pre/post-processing.
- **Run-time side effects** — `cloudpickle.register_pickle_by_value(...)`, global schema flags, choosing the executor dynamically.
- **Non-standard execution** — more than one `Processor`, `apply_to_fileset` instead of `Runner`, an extra call argument such as `treename=`, or a `Processor` built from something live/unpicklable (a DB handle, or a histserv `remote_hist`).
- **Non-static `Processor` args** — anything you can't express as plain cache-stable `processor_params`.

Rule of thumb: if `processor=` + `processor_params=` + `runner_params=` describe your analysis fully with nothing left over, delete the function. The moment the wrapper contains a *decision* or a *side effect*, keep it. The [simple example](https://github.com/CoffeaTeam/coffea-workflow/tree/main/examples/coffea_workflow/) is declarative; [examples/agc/](https://github.com/CoffeaTeam/coffea-workflow/tree/main/examples/agc/) and both histserv examples keep a function (dynamic run-time setup, and a live `remote_hist`, respectively).

---

### Artifacts

An **Artifact** is the typed, hashable representation of one unit of work and its output. The executor stores every artifact at:

```
<cache_dir>/<type_name>/<identity>/
```

**External artifacts** (declared in `Step`, user-visible):

| Artifact | Description |
|---|---|
| `Fileset` | Entry point. Builder returns a standard coffea fileset dict. Cached as `fileset.json`. |
| `Analysis` | Central stage. Orchestrates chunking, runs your analysis function per chunk, merges results. Returns `payload.pkl`. |
| `Plotting` | Consumes merged `Analysis` output. Always re-runs (`always_rerun = True`) — plots are fast and expected fresh. |

**Internal artifacts** (created automatically, never user-facing):

| Artifact | Description |
|---|---|
| `Chunking` | Splits the `Fileset` into `fileset_chunk_N.json` files per the configured strategy. |
| `ChunkAnalysis` | Processes one chunk. Writes `.success` on success; its absence triggers a retry on the next run. |

---

## RunConfig
 
`RunConfig` is the single configuration object passed to `run()`.

```python
@dataclass(frozen=True)
class RunConfig:
    strategy: "by_dataset" | None = None
    percentage: int | None = None
    datasets: tuple[str, ...] | None = None
    cache_dir: Path = Path(".cache")
    facility: FacilityBase | None = None
    executor_config: ExecutorConfig | None = None
    hist_client: Any | None = None
    hist_template: str | Callable | None = None
    histserv_token: str | None = None
    histserv_connection_info: dict | None = None
```

| Field | Type | Default | Description |
|---|---|---|---|
| `strategy` | `"by_dataset"` or `None` | `None` | `"by_dataset"` → one chunk per dataset; `None` → all datasets together |
| `percentage` | `int` or `None` | `None` | Each chunk covers this % of each dataset's files (must divide 100 evenly, e.g. 20, 25, 50) |
| `datasets` | `tuple[str, ...]` or `None` | `None` | Restrict to named datasets only; accepts a list (auto-converted to tuple) |
| `cache_dir` | `Path` | `Path(".cache")` | Root of the content-addressable store |
| `facility` | `FacilityBase` or `None` | `None` | Which facility factory to use (local, coffea-casa, lxplus) |
| `executor_config` | `ExecutorConfig` or `None` | `None` | Fine-grained executor control (type, workers) |
| `hist_client` | `histserv.Client` or `None` | `None` | Live histserv client for remote histogram accumulation |
| `hist_template` | `str \| Callable` or `None` | `None` | `'module:function'` (or callable) returning the local `hist.Hist`/`ChunkedHist` to register. Required when `hist_client` is set — the framework calls it to create the histogram, and again to replace it if a later run finds the connection expired |
| `histserv_token` | `str` or `None` | `None` | Optional access token used when (re)creating a histogram |
| `histserv_connection_info` | `dict` or `None` | `None` | Manual override pointing at an existing server-side histogram. Normally left `None` — see below |

---
 
### Producers

A **producer** is the framework function that materialises an artifact. Users never write producers — they only write the **builder functions** that producers call. Built-in producers handle splitting, caching, merging, and executor selection automatically.

---
 
### Executor

`Executor` is instantiated once per `run()` call. For each artifact it checks the cache and either returns the cached result or calls the appropriate producer to materialise it.

| Artifact | Cache sentinel | Re-run condition |
|---|---|---|
| `Fileset` | `fileset.json` | inputs changed |
| `Chunking` | `manifest.json` | inputs changed |
| `ChunkAnalysis` | `.success` | `.success` absent |
| `Analysis` | `payload.pkl` + no `.has_failures` | `.has_failures` present |
| `Plotting` | — | always (`always_rerun = True`) |

---
 
## run
 
`run(workflow: Workflow, config: RunConfig) -> dict` is the single entry point that the user specifies to execute the entire workflow:
 
```python
result = run(workflow, config)
```

Runs a **topological sort**  over the step graph, materializes needed artifacts and prints the summary.

---
 
## histserv Integration

[histserv](https://github.com/pfackeldey/histserv) is a remote histogram accumulation server. When configured, the `Analysis` producer routes chunk results to the server instead of merging locally.

```python
import histserv
from coffea_workflow import detect_histserv_address

# detect_histserv_address() picks the right server for the coffea-casa site this code
# runs on (Nebraska vs UChicago today); pass override="host:port" to skip detection.
hist_client = histserv.Client(address=detect_histserv_address())

config = RunConfig(
    hist_client=hist_client,
    hist_template="analysis:hist_template",   # 'module:function' (or callable), no args -> hist.Hist
    histserv_token="test",                     # optional
    strategy="by_dataset",
    percentage=20,
)
```

Your `run_analysis` and `plot_results` functions accept a `config` keyword argument to read `config.histserv_connection_info` and reconnect to the remote histogram.

**No manual `hist_client.init()`, and nothing to carry between runs.** The framework creates the histogram on first use (via `hist_template()`) and reconnects to the *same* one — identified by the `Analysis` step's cache identity — on every later run with the same `cache_dir`. histserv doesn't expose an expiry timestamp to the client (idle histograms are pruned server-side, default 24h, but the actual server config isn't queryable), so expiry is discovered by trying to reconnect: if the previous histogram is gone, the framework transparently creates a new one and prints that it did so, so a silent discontinuity in results is never hidden.

To point at an existing histogram explicitly instead (e.g. one a colleague created), pass `histserv_connection_info` manually — the framework validates it the same way and still auto-recreates if it's since expired.

See [examples/coffea_workflow_histserv/](https://github.com/CoffeaTeam/coffea-workflow/tree/main/examples/coffea_workflow_histserv/) for a full worked example.

---

## Examples

| Location | What it shows |
|---|---|
| [examples/showcase/split_strategy/](https://github.com/CoffeaTeam/coffea-workflow/tree/main/examples/showcase/split_strategy/) | One notebook per split strategy, sharing a common analysis |
| [examples/showcase/facilities/](https://github.com/CoffeaTeam/coffea-workflow/tree/main/examples/showcase/facilities/) | Switching between local, coffea-casa, and lxplus |
| [examples/showcase/optimisation/](https://github.com/CoffeaTeam/coffea-workflow/tree/main/examples/showcase/optimisation/) | Sequential vs parallel execution benchmarks (in progress) |
| [examples/coffea_workflow/](https://github.com/CoffeaTeam/coffea-workflow/tree/main/examples/coffea_workflow/) | Simple accumulator workflow (no histserv) |
| [examples/coffea_workflow_histserv/](https://github.com/CoffeaTeam/coffea-workflow/tree/main/examples/coffea_workflow_histserv/) | Same workflow with the histserv histogram server |
| [examples/agc/workflow_coffea_casa.ipynb](https://github.com/CoffeaTeam/coffea-workflow/tree/main/examples/agc/workflow_coffea_casa.ipynb) | Full AGC ttbar analysis |
| [examples/agc/workflow_coffea_casa_histserv.ipynb](https://github.com/CoffeaTeam/coffea-workflow/tree/main/examples/agc/workflow_coffea_casa_histserv.ipynb) | Same AGC ttbar analysis, region histograms streamed to histserv |

---

## Acknowledgements

`coffea-workflow` was developed by Yana Holoborodko. Contributions, bug reports, and feedback are welcome via GitHub issues.
