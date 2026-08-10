"""
Custom event-level preprocessing: turn a fileset into a list of
coffea WorkItems (one per event range), bypassing Runner's internal
chunking entirely.

coffea's ``Runner.run()`` accepts a premade ``list[WorkItem]`` and dispatches
each item as one independent executor task (one Dask task per WorkItem),
skipping its own metadata fetch and chunksize-based chunk generation. This
module produces such lists:

  - ``build_workitems`` opens every file once (in parallel via a Dask client
    when one is available), reads the entry count and file uuid, optionally
    runs a user-supplied ``custom_func(uproot_file) -> dict`` for extra
    per-file metadata (e.g. sum-of-weights bookkeeping histograms), and cuts
    each file into ``step_size``-event ranges using the same arithmetic as
    ``coffea.dataset_tools.preprocess``.
  - ``split_workitems`` partitions the (serialized) items with the same
    strategy/percentage semantics as ``coffea.dataset_tools.splitting
    .split_fileset``, so a single-file dataset still splits into chunks.
  - ``workitems_to_json`` / ``workitems_from_json`` round-trip WorkItems
    through plain JSON (the binary file uuid is base64-encoded).
  - ``hash_workitems`` gives a stable content hash for cache keys.
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
import warnings

import uproot
from coffea.processor.executor import WorkItem


def _normalize_files(fileset, treename):
    """
    Return ``{dataset: {"files": {path: treename}, "metadata": dict}}``.

    Accepts the same shapes as coffea's Runner: bare list of paths,
    ``{"files": [path, ...]}`` with a dataset-level or supplied ``treename``,
    or ``{"files": {path: treename}}``.
    """
    normalized = {}
    for name, data in fileset.items():
        if isinstance(data, (list, tuple)):
            data = {"files": list(data)}
        if not isinstance(data, dict):
            raise TypeError(
                f"Dataset '{name}': expected a dict or list, got {type(data).__name__}"
            )
        files = data.get("files")
        if isinstance(files, (list, tuple)):
            local_treename = data.get("treename") or treename
            if local_treename is None:
                raise ValueError(
                    f"Dataset '{name}' uses list-format files without tree names; "
                    "set RunConfig.treename (e.g. treename='Events') so "
                    "preprocessing knows which TTree to open."
                )
            files = {path: local_treename for path in files}
        elif not isinstance(files, dict):
            raise TypeError(
                f"Dataset '{name}': 'files' must be a dict or list, "
                f"got {type(files).__name__}"
            )
        normalized[name] = {
            "files": files,
            "metadata": data.get("metadata") or {},
        }
    return normalized


def extract_file_metadata(fname_and_treename, custom_func=None):
    """
    Open one file and extract what WorkItem construction needs.

    Runs on a Dask worker when a client is available. ``custom_func`` receives
    the open uproot file and must return a JSON-serializable dict; its result
    is merged into every WorkItem's usermeta for that file.

    ``custom_func`` may arrive as a cloudpickle byte blob (see
    :func:`build_workitems`): it is unpickled here, on the worker, so the user
    module it references is never imported on the Dask scheduler.
    """
    fname, treename = fname_and_treename
    if isinstance(custom_func, (bytes, bytearray)):
        import cloudpickle

        custom_func = cloudpickle.loads(custom_func)
    with uproot.open(fname) as f:
        meta = {
            "fileuuid": f.file.uuid.bytes,
            # handle missing trees the same way the reference implementation does
            "num_entries": f[treename].num_entries if treename in f else 0,
            "custom_meta": custom_func(f) if custom_func is not None else {},
        }
    return {fname: meta}


def build_workitems(fileset, step_size, treename=None, custom_func=None, client=None):
    """
    Preprocess a fileset into a list of WorkItems of ~step_size events each.

    Parameters
    ----------
        fileset : dict
            ``{dataset: {"files": {path: treename} | [path, ...],
            "metadata": {...}}}`` (Runner-style).
        step_size : int
            Target number of events per WorkItem. Files are cut with the same
            arithmetic coffea uses: ``n_steps = max(round(n / step_size), 1)``,
            then even ceil-sized ranges.
        treename : str or None
            Fallback tree name for list-format files.
        custom_func : callable or None
            ``custom_func(uproot_file) -> dict`` extra per-file metadata,
            merged into usermeta (dataset metadata keys are overridden on
            collision, matching ``metadata | custom_meta`` semantics).
        client : distributed.Client or None
            When given, files are opened in parallel on the cluster.

    Returns
    -------
        list[WorkItem]
    """
    normalized = _normalize_files(fileset, treename)

    to_extract = sorted(
        {
            (path, tree)
            for data in normalized.values()
            for path, tree in data["files"].items()
        }
    )
    # A user-supplied custom_func is embedded in the task graph that client.map
    # submits, and Dask deserializes that graph on the *scheduler*. If the
    # callable travels by reference, the scheduler must import its defining
    # module — running that module's top-level imports (e.g. the analysis's
    # ``import utils``), which are not installed there — and fails while
    # "deserializing the task graph". distributed serializes tasks with plain
    # pickle first, so cloudpickle.register_pickle_by_value does NOT help here.
    #
    # Instead, cloudpickle the callable into an opaque bytes blob on the driver
    # and pass the blob as data (the same trick coffea uses to ship a processor
    # via heavy_input). Bytes are never unpickled by the scheduler; the blob is
    # only loaded on the worker — which has the user module — inside
    # extract_file_metadata. Serial runs keep the live callable, no client.
    extract_func = custom_func
    if custom_func is not None and client is not None:
        import cloudpickle

        extract_func = cloudpickle.dumps(custom_func)

    def _extract(fname_and_treename):
        from coffea_workflow.preprocessing import extract_file_metadata
        return extract_file_metadata(fname_and_treename, custom_func=extract_func)

    if client is not None:
        metas = client.gather(client.map(_extract, to_extract))
    else:
        metas = [_extract(entry) for entry in to_extract]
    meta_by_file = {fname: meta for result in metas for fname, meta in result.items()}

    workitems = []
    for dataset, data in normalized.items():
        ds_meta = data["metadata"]
        for path, tree in sorted(data["files"].items()):
            file_meta = meta_by_file[path]
            num_entries = file_meta["num_entries"]
            if num_entries == 0:
                warnings.warn(
                    f"File '{path}' in dataset '{dataset}' has no entries "
                    f"in tree '{tree}' (or the tree is missing); skipping."
                )
                continue
            # same step arithmetic as coffea.dataset_tools.preprocess.get_steps
            n_steps = max(round(num_entries / step_size), 1)
            actual_step_size = math.ceil(num_entries / n_steps)
            for i in range(n_steps):
                start = i * actual_step_size
                stop = min((i + 1) * actual_step_size, num_entries)
                if stop - start == 0:
                    continue
                workitems.append(
                    WorkItem(
                        dataset=dataset,
                        filename=path,
                        treename=tree,
                        entrystart=int(start),
                        entrystop=int(stop),
                        fileuuid=file_meta["fileuuid"],
                        usermeta={**ds_meta, **file_meta["custom_meta"]},
                    )
                )
    return workitems


def workitems_to_json(workitems):
    """Serialize WorkItems to JSON-safe records (fileuuid base64-encoded)."""
    records = []
    for wi in workitems:
        records.append(
            {
                "dataset": wi.dataset,
                "filename": wi.filename,
                "treename": wi.treename,
                "entrystart": wi.entrystart,
                "entrystop": wi.entrystop,
                "fileuuid": base64.b64encode(wi.fileuuid).decode("ascii"),
                "usermeta": wi.usermeta,
            }
        )
    return records


def workitems_from_json(records):
    """Rebuild WorkItems from records produced by :func:`workitems_to_json`."""
    return [
        WorkItem(
            dataset=r["dataset"],
            filename=r["filename"],
            treename=r["treename"],
            entrystart=r["entrystart"],
            entrystop=r["entrystop"],
            fileuuid=base64.b64decode(r["fileuuid"]),
            usermeta=r.get("usermeta"),
        )
        for r in records
    ]


def _record_sort_key(record):
    return (record["dataset"], record["filename"], record["entrystart"])


def split_workitems(records, strategy=None, datasets=None, percentage=None):
    """
    Partition serialized WorkItem records into chunks.

    Mirrors ``coffea.dataset_tools.splitting.split_fileset`` semantics, with
    one (file, entry-range) WorkItem as the unit instead of a whole file:

      - ``strategy="by_dataset"`` alone: one chunk per dataset.
      - ``percentage=p`` alone: ``100/p`` chunks, each with ``p`` percent of
        every dataset's items (mixed chunks).
      - both: ``N_datasets * (100/p)`` chunks, not mixed.
      - ``datasets`` (list/tuple or callable) restricts to selected datasets.

    Returns a list of record lists, deterministically ordered.
    """
    if strategy is not None and strategy != "by_dataset":
        raise ValueError(f"Unknown strategy '{strategy}'. Use 'by_dataset' or None.")
    if percentage is not None:
        if (
            not isinstance(percentage, int)
            or not (1 <= percentage <= 100)
            or 100 % percentage != 0
        ):
            raise ValueError(
                "'percentage' must be an int that divides 100 evenly (e.g. 10, 20, 25, 50)."
            )

    if datasets is None:
        pass
    elif callable(datasets):
        records = [r for r in records if datasets(r["dataset"])]
    else:
        keep = set(datasets)
        records = [r for r in records if r["dataset"] in keep]

    by_dataset = {}
    for record in sorted(records, key=_record_sort_key):
        by_dataset.setdefault(record["dataset"], []).append(record)

    if strategy == "by_dataset":
        groups = [{name: items} for name, items in by_dataset.items()]
    else:
        groups = [by_dataset]

    if percentage is None:
        return [
            [record for items in group.values() for record in items]
            for group in groups
            if any(group.values())
        ]

    n_chunks = 100 // percentage
    result = []
    for group in groups:
        for bin_idx in range(n_chunks):
            chunk = []
            for items in group.values():
                n = len(items)
                if n == 0:
                    continue
                chunk_size = max(1, math.ceil(n / n_chunks))
                start = bin_idx * chunk_size
                end = min(start + chunk_size, n)
                if start >= n:
                    continue
                chunk.extend(items[start:end])
            if chunk:
                result.append(chunk)
    return result


def hash_workitems(records):
    """Stable SHA-256 over the content of one chunk of WorkItem records."""
    canonical = sorted(
        (
            {
                "dataset": r["dataset"],
                "filename": r["filename"],
                "treename": r["treename"],
                "entrystart": r["entrystart"],
                "entrystop": r["entrystop"],
                "fileuuid": r["fileuuid"],
                "usermeta": r.get("usermeta"),
            }
            for r in records
        ),
        key=_record_sort_key,
    )
    serialized = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(serialized).hexdigest()
