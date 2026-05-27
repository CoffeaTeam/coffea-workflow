"""
This module contains functions that are applied to the DAG by snakemake_render
"""

from .producers_utils import _call_builder, _extract_acc, _load_object, _split_fileset, build_executor
from coffea.dataset_tools.splitting import hash_fileset

from typing import Any
from pathlib import Path
from .config import RunConfig
import cloudpickle
from coffea.processor import accumulate
import json

def make_fileset_standalone(
    builder: str | Any,
    out_path: Path | str,
    builder_params: dict | None = {},
    config: RunConfig | None = None,
) -> None:
    """
    Call the fileset builder and write the result as JSON to out_path.
    """
    fn = _load_object(builder)
    fileset_dict = _call_builder(fn, config=config, builder_params=builder_params)
    if not isinstance(fileset_dict, dict):
        raise TypeError("Fileset builder must return a dict")
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(fileset_dict, indent=2, sort_keys=True))


def split_fileset_standalone(
    fileset_path: Path | str,
    out_dir: Path | str,
    strategy: str | None = None,
    percentage: int | None = None,
    datasets: list | None = None,
) -> None:
    """Split a fileset JSON file into chunk files inside out_dir.

    Writes ``chunk_0.json``, ``chunk_1.json``, … and a ``manifest.json``
    with the list of output files and total chunk count.
    """
    fileset = json.loads(Path(fileset_path).read_text())
    chunks = _split_fileset(fileset, strategy=strategy, datasets=datasets, percentage=percentage)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_files = {}
    for i, chunk in enumerate(chunks):
        fname = f"chunk_{i}.json"
        (out_dir / fname).write_text(json.dumps(chunk, indent=2, sort_keys=True))
        manifest_files[str(i)] = {
            "file": fname,
            "hash": hash_fileset(chunk),
        }
    (out_dir / "manifest.json").write_text(
        json.dumps({"output_files": manifest_files, "n_chunks": len(chunks)}, indent=2, sort_keys=True)
    )


def run_chunk_analysis_standalone(
    chunk_path: Path | str,
    builder: str | Any,
    out_payload: Path | str,
    out_status: Path | str,
    builder_params: dict | None = {},
    config: RunConfig | None = None,
) -> None:
    """Run the user's analysis on one chunk file.

    Always writes both *out_payload* (cloudpickle) and *out_status* (text
    file containing ``"ok"`` or ``"failed: <reason>"``).  The rule therefore
    always satisfies Snakemake's output requirements regardless of whether the
    analysis succeeded — failed chunks are tracked via the status file and
    skipped during merging (Option A partial-result strategy).
    """
    fn = _load_object(builder)
    chunk_fileset = json.loads(Path(chunk_path).read_text())
    try:
        print("Before runner")
        executor = build_executor(
            config.executor_config if config is not None else None,
            config.facility if config is not None else None,
        )
        result = _call_builder(fn, chunk_fileset, config=config, executor=executor, builder_params=builder_params)
        print("After runner")
        status = "ok" if result.is_ok() else f"failed: {result}"
    except Exception as exc:
        result = None
        status = f"failed: {exc}"
    out_payload = Path(out_payload)
    out_payload.parent.mkdir(parents=True, exist_ok=True)
    out_payload.write_bytes(cloudpickle.dumps(result))
    Path(out_status).write_text(status)


def merge_chunk_results_standalone(
    chunk_payloads: list[Path | str],
    chunk_statuses: list[Path | str],
    builder_key: str,
    out_path: Path | str,
    config: RunConfig | None = None,
) -> None:
    """
    Merge successful chunk payloads into a single Analysis payload file.

    Reads each `.status` file; payloads whose status starts with "ok"
    are accumulated.  Failed chunks are recorded in the `failures` list inside the output payload.
    """
    merged_acc = None
    metrics_merged = None
    failures = []
    for payload_path, status_path in zip(chunk_payloads, chunk_statuses):
        status = Path(status_path).read_text().strip()
        if not status.startswith("ok"):
            failures.append({"chunk_file": str(payload_path), "error": status})
            continue
        result = cloudpickle.loads(Path(payload_path).read_bytes())
        if result is not None and result.is_ok():
            acc, metrics = _extract_acc(result)
            if config is not None and config.hist_client is not None:
                merged_acc = config.histserv_connection_info
            else:
                merged_acc = accumulate([acc], accum=merged_acc)
            metrics_merged = accumulate([metrics], accum=metrics_merged)
        else:
            failures.append({"chunk_file": str(payload_path), "error": str(result)})
    payload = {
        "builder": builder_key,
        "n_chunks_total": len(chunk_payloads),
        "n_chunks_ok": len(chunk_payloads) - len(failures),
        "failures": failures,
        "processor_result": (merged_acc, metrics_merged),
    }
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(cloudpickle.dumps(payload))


def make_plot_standalone(
    merged_payload_path: Path | str,
    builder: str | Any,
    out_path: Path | str,
    builder_params: dict | None = {},
    config: RunConfig | None = None,
) -> None:
    """
    Call the plot builder with the merged analysis payload and write its result.
    """
    payload = cloudpickle.loads(Path(merged_payload_path).read_bytes())
    fn = _load_object(builder)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    builder_params.update({'output_path':out_path})
    if config is not None and config.histserv_connection_info is not None:
        plot_result = _call_builder(fn, config=config, builder_params=builder_params)
    else:
        plot_result = _call_builder(fn, payload, builder_params=builder_params)
    
    out_path.write_bytes(cloudpickle.dumps(plot_result))
