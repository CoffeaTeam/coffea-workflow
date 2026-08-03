# Workflow with histserv

The same accumulator workflow as [../coffea_workflow/](../coffea_workflow/), but histograms are streamed to a histserv histogram server during processing instead of being merged locally.

- [analysis_hist.py](analysis_hist.py) — the builder functions plus `hist_template()`, which defines the histogram registered with the server
- [workflow_hist.ipynb](workflow_hist.ipynb) — connects a `histserv.Client` and runs the workflow on coffea-casa

Key differences from the plain accumulator version:

- A `histserv.Client` is created up front (address picked automatically via `detect_histserv_address()` — see below), and passed as `hist_client` in `RunConfig` along with `hist_template` (and optionally `histserv_token`)
- Builders that declare a `config` parameter receive the `RunConfig` automatically, so `run_analysis(fileset, config)` can read `config.histserv_connection_info` and `plot_results(config)` can fetch the final histogram from the server
- **No manual `hist_client.init()` and no connection info to carry between runs.** The framework creates the histogram on first use and reconnects to the *same* one on every later run of this workflow/`cache_dir` — you don't pass anything back in by hand. If the server has since pruned it (histserv doesn't expose the exact expiry to the client — it's discovered by trying to reconnect), the framework transparently creates a new histogram and prints that it did so, so a silent discontinuity in results never goes unnoticed.

## Automatic histserv address (`detect_histserv_address`)

```python
from coffea_workflow import detect_histserv_address

hist_client = histserv.Client(address=detect_histserv_address())
```

Picks the right server address for the coffea-casa site the code is currently running on (Nebraska vs UChicago today), based on a best-effort read of `/etc/resolv.conf`/hostname — not a documented API, so it raises with a clear message if the site can't be determined rather than guessing wrong. Pass an explicit address to skip detection entirely: `detect_histserv_address(override="host:port")`.

Requires `pip install histserv`. See the histserv section of the [main README](../../README.md) for details.
