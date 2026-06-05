# Split strategies

`coffea-workflow` can split your fileset into independent chunks before running the analysis. 
Each chunk is processed and cached independently — so if some chunks fail, you still get the merged
partial result from all successful chunks (thanks to `use_result_type=True` in coffea's `Runner`), 
and a rerun will only attempt the failed chunks rather than starting everything from scratch.

## Available strategies

### No split (`strategy=None`)

The entire fileset is passed to the executor as one unit. Coffea's `Runner` internally distributes 
files across Dask workers (or processes), but the whole run is treated as a single job.

```python
RunConfig(strategy=None)
```

### By dataset (`strategy="by_dataset"`)

The fileset is split into one chunk per dataset. Each dataset is processed independently.

```python
RunConfig(strategy="by_dataset")
```

### By dataset + percentage (`strategy="by_dataset"`, `percentage=50`)

Each dataset is further split into N chunks based on what percentage of files each chunk contains. `percentage=50` → 2 chunks per dataset,
`percentage=25` → 4 chunks per dataset. The value must divide 100 evenly.

```python
RunConfig(strategy="by_dataset", percentage=50)
```

### Mixed chunks — percentage only (`strategy=None`, `percentage=50`)

Files are split into chunks by percentage without grouping by dataset, so each chunk can contain files from multiple datasets mixed together.
Useful for testing how your analysis handles cross-dataset inputs or for running quick sanity checks across a representative slice of the full
fileset.

```python
RunConfig(strategy=None, percentage=50)
```

## Trade-offs

| | No split | By dataset | By dataset + % | Mixed (% only) |
|---|---|---|---|---|
| Chunks | 1 | 1 per dataset | N per dataset | N across all files |
| Dataset grouping | no | yes | yes | no — datasets mixed |
| Fault tolerance | none | per dataset | per chunk | per chunk |
| Cache granularity | whole run | per dataset | per chunk | per chunk |
| Overhead | lowest | low | higher | higher |
| Best for | small filesets | multi-dataset runs | large filesets, failure-prone | cross-dataset testing |

**Smaller chunks preserve more work on failure** — if processing fails halfway through, only the failed chunk needs to be rerun, and already-completed chunks are read from cache.

**However**, smaller chunks can be less efficient depending on the facility and executor. With `DaskExecutor` on lxplus, submitting many small HTCondor jobs has higher scheduling overhead than a few large ones. With `FuturesExecutor` locally, the overhead is negligible.

## Examples

- [workflow_no_split.ipynb](workflow_no_split.ipynb) — `strategy=None`
- [workflow_by_dataset.ipynb](workflow_by_dataset.ipynb) — `strategy="by_dataset"`
- [workflow_by_dataset_percentage.ipynb](workflow_by_dataset_percentage.ipynb) — `strategy="by_dataset"`, `percentage=50`
- [workflow_mixed_percentage.ipynb](workflow_mixed_percentage.ipynb) — `strategy=None`, `percentage=50`
