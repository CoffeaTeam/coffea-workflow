# AGC Example with `coffea-workflow`

This example demonstrates how an existing [AGC](https://github.com/iris-hep/analysis-grand-challenge/tree/main/analyses/cms-open-data-ttbar) coffea analysis example can be plugged into the
`coffea-workflow` module with minimal changes to quickly gain automatic caching, chunk-level fault
tolerance, partial result, partial run and a clean DAG-based execution model for further mapping to a workflow language if wanted.

## What changed in the analysis code

Almost nothing. The first requirement is that the analysis logic lives in three
plain Python functions with the right signatures. The user should remember to specify what the previous function returns and what the next one accepts:

| Function | Signature | What it does |
|---|---|---|
| `get_fileset()` | `() → dict` | Returns the XRootD fileset dictionary |
| `run_analysis(fileset)` | `(dict) → result(Accumulatable + metrics(optional) or histserv connection information)` | Runs the coffea processor on one chunk |
| `plot_results(result)` | `(result) → None` | Plots the merged accumulator |

The second requirement is to use Result type (Ok or Err) of coffea processor:
```python
run = processor.Runner(
                            executor=executor,
                            schema=NanoAODSchema,
                            savemetrics=True,
                            metadata_cache={},
                            chunksize=utils.config["benchmarking"]["CHUNKSIZE"],
                            use_result_type=True # CHANGED
                        )
```


The functions are referenced by the workflow as plain callables.
The example of this small code restructure can be found in [ttbar_analysis.py](https://github.com/hooloobooroodkoo/coffea-workflow/blob/main/examples/agc_ttbar/ttbar_analysis.py).

## How does the workflow look
Functions from the analysis file should be mapped to a corresponding step_type. See in the example:
```python
from workflow import Step, Workflow, Fileset, Analysis, Plotting, RunConfig, run
from ttbar_analysis import get_fileset, run_analysis, plotting_1

# Step 1. defines steps, map them with your functions
step_fileset = Step(
							        name="Fileset_ttbar", # your custom name for a step
							        step_type = Fileset, # predefined step type that calls the corresponding backend function
							        builder = get_fileset, # your code fthat creates and returns the fileset
							    )
step_analysis = Step(
						        name="Analysis_ttbar",
						        step_type = Analysis,
						        builder = run_analysis,
						    )

step_plot1 = Step(
						        name="Plot1_ttbar",
						        step_type = Plotting,
						        builder = plotting_1,
						    )

# Step 2. Put together the DAG
workflow = Workflow()
workflow.add(step_fileset)
workflow.add(step_analysis, depends_on=[step_fileset])
workflow.add(step_plot1, depends_on=[step_analysis])

# Step 3. How to extract the workflow?
# percentage=10 + strategy="by_dataset" -> datasets_number*(100/10) chunks, every chunk is processed separately, if one fails, others produce and return partial merged result
# cache_dir preserves partial result and only processes the failed or unproduced chunks when the notebook is rerun
# chunk_fraction=0.3 ~ test mode; try analysis on 30% of chunks; save partial result is success, then remove this parameter and produce the complete result without rerunning this 30% of chunks
config = RunConfig(percentage=10, strategy="by_dataset", cache_dir="cache", chunk_fraction=0.3)

# Step 4. Run while applying config
result = run(workflow, config)
```
Example:
```bash
Split strategy applied, starting independent processing of 43 fileset subsets...

chunk_fraction=0.3: processing 13 of 43 chunks
------------------------------------
Processing fileset_chunk_0.json
Extracted from cache: cache/ChunkAnalysis/27afae63d2c0e5179b8a82d0d59cdebc1701b47e59d097e461cc1bbf65675bde
Successfully processed!
------------------------------------
Processing fileset_chunk_1.json
Extracted from cache: cache/ChunkAnalysis/baf7fb8a9c7c5e19d7879d6ef04e7cc2f3b7ecac0607fc391f9e2b7835dfc0e2
Successfully processed!
```

## What the `workflow` module adds

### Control over execution

```bash
Split strategy applied, starting independent processing of 43 fileset subsets...

chunk_fraction=0.3: processing 13 of 43 chunks
```

### Artifact caching

Every step — `Fileset`, `Analysis`, `Plotting` — is hashed by its inputs and
builder. If nothing changed, the step is loaded from cache instead of rerun.
Rerunning the notebook after a successful execution is instant. Rerun without  after test run chunk_fraction with 0.0 < chunk_fraction < 1.0 will extract
earlier produced results and only rerun the remaining.
```bash
cache/
Fileset/d7214e0...   ← fileset.json
Analysis/62fd4ec...  ← payload.pkl (merged accumulator)
Plotting/de3559d...  ← payload.pkl
```

### Chunk-level fault tolerance

The `Analysis` step splits the fileset into chunks (by dataset or by percentage)
and processes each independently. A failed chunk is reported but does not abort
the run — the rest continue. On the next run, only the missing chunks are
reprocessed.
```bash
=== Run Summary ===
!  SingleMuonAnalysis         Analysis    4/5 chunks OK
FAILED fileset_chunk_0.json: [FATAL] Invalid address
✓  PlottingMuonAnalysis       Plotting
```

