from __future__ import annotations
import json
from pathlib import Path
from typing import Any
import cloudpickle
from .artifacts import Fileset, Analysis, Chunking, ChunkAnalysis, Plotting, CustomArtifact, _builder_key
from .deps import Deps
from .producers import producer
from .config import RunConfig
from coffea.processor import accumulate
from coffea.dataset_tools.splitting import hash_fileset
from .producers_utils import _call_builder, _extract_acc, _load_object, _split_fileset, _load_artifact_output, _safe_print

@producer(Fileset)
def make_fileset(*, art: Fileset, deps: Deps, out: Path, config: RunConfig) -> None:
    # finds and calls the function that user specified in builder
    fn = _load_object(art.builder)
    fileset_dict = _call_builder(fn, builder_params=dict(art.builder_params))

    if not isinstance(fileset_dict, dict):
        raise TypeError("Fileset builder must return a dict")

    out.mkdir(parents=True, exist_ok=True) # a folder for the Artifact (name is identity())
    (out / "fileset.json").write_text(json.dumps(fileset_dict, indent=2, sort_keys=True))


@producer(Chunking)
def split_fileset(*, art: Chunking, deps: Deps, out: Path, config: RunConfig) -> None:
    out.mkdir(parents=True, exist_ok=True)
    fileset = _load_artifact_output(art.fileset, deps.need(art.fileset))
    if not isinstance(fileset, dict):
        raise TypeError(
            f"Upstream artifact '{art.fileset.type_name}' must produce a fileset dict, "
            f"got {type(fileset).__name__}"
        )

    chunks = _split_fileset(
        fileset,
        strategy=config.strategy,
        datasets=list(config.datasets) if config.datasets else None,
        percentage=config.percentage,
    )

    manifest_files = {}
    for i, chunk in enumerate(chunks):
        file_name = f"fileset_chunk_{i}.json"
        (out / file_name).write_text(json.dumps(chunk, indent=2, sort_keys=True))
        manifest_files[str(i)] = {
            "file": file_name,
            "hash": hash_fileset(chunk),
        }

    (out / "manifest.json").write_text(json.dumps({
        "output_files": manifest_files,
        "n_chunks": len(chunks),
    }, indent=2, sort_keys=True))

    
@producer(ChunkAnalysis)
def run_analysis(*, art: ChunkAnalysis, deps: Deps, out: Path, config: RunConfig) -> None:
    """
    Apply user's analysis to one chunk.
    """
    out.mkdir(parents=True, exist_ok=True)

    # create chunks applying splitting strategy
    # TODO: do I need chunking initialisation again? is it enough to just have it in execute_analysis()?
    chunking_dir = deps.need(art.chunking)  # directory with chunk jsons
    chunk_path = chunking_dir / art.chunk_file
    chunk_fileset = json.loads(chunk_path.read_text())

    fn = _load_object(art.analysis_builder)  # user's function
    executor = deps.coffea_executor()
    result = _call_builder(fn, chunk_fileset, config=config, executor=executor,
                           builder_params=dict(art.builder_params))

    (out / "payload.pkl").write_bytes(cloudpickle.dumps(result))
    if result.is_ok():
        (out / ".success").touch()

@producer(Analysis)
def execute_analysis(*, art: Analysis, deps: Deps, out: Path, config: RunConfig) -> None:
    """
    This should execute Chunking and run the analysis per chunk + merging
    """
    # create chunks applying splitting strategy
    # it's an artifact that user is not using - internal
    chunking = Chunking(
        fileset=art.fileset,
        split_strategy=config.strategy,
        percentage=config.percentage,
        datasets=config.datasets,
    )
    chunk_dir = deps.need(chunking) # self._executor.materialize(Chunking); returns path to .cache_dir / Chunking / hash where all .json chunks are
    manifest_path = chunk_dir / "manifest.json" # manifest contains info about our fileset.json or its chunks .json

    manifest = json.loads(manifest_path.read_text())
    chunks_entries = list(manifest["output_files"].values())

    chunks_files_num = manifest["n_chunks"]
    if chunks_files_num > 1:
        _safe_print(f"\nSplit strategy {config.strategy!r}: processing {chunks_files_num} fileset subsets independently...\n")
    else:
        _safe_print(f"\nNo split strategy — processing the whole fileset as one...")

    if config.chunk_fraction is not None:
        n = max(1, round(len(chunks_entries) * config.chunk_fraction))
        chunks_entries = chunks_entries[:n]
        _safe_print(f"chunk_fraction={config.chunk_fraction}: processing {n} of {manifest['n_chunks']} chunks")

    merged_acc = None
    metrics_merged = None
    failures = []

    coffea_exec = deps.coffea_executor()
    wants_parallel = config.executor_config is not None and config.executor_config.parallel_chunks
    if wants_parallel and not hasattr(coffea_exec, "client"):
        raise ValueError(
            "parallel_chunks=True requires a DaskExecutor. "
            "Set executor_type='DaskExecutor' in ExecutorConfig."
        )
    if wants_parallel and config.hist_client is not None:
        raise ValueError(
            "parallel_chunks=True is not compatible with hist_client: "
            "the histserv gRPC connection cannot be serialized to Dask workers. "
            "Use the default sequential mode when streaming to a hist server."
        )
    use_parallel = wants_parallel

    if use_parallel:
        # Defined as a nested function so cloudpickle serializes it as bytecode,
        # not as a module reference — the scheduler/workers don't have coffea_workflow installed.
        def _run_chunk_remote(chunk_fileset, builder_bytes, builder_params):
            """
            Runs on a Dask worker. No coffea_workflow imports — only coffea is required.
            It's a serializable wrapper that replicates what run_analysis + _call_builder do locally,
            but without importing coffea_workflow (which may not be installed on workers).
            """
            import cloudpickle, inspect

            # hist.Hist.identity() was required by coffea's old accumulator protocol but was
            # removed from the hist package. IterativeExecutor hits this when merging per-file
            # results inside a chunk. Restore it so the worker's coffea can accumulate.
            try:
                import hist as _hist
                if not hasattr(_hist.Hist, "identity"):
                    def _hist_identity(self):
                        h = self.copy()
                        h.reset()
                        return h
                    _hist.Hist.identity = _hist_identity
            except ImportError:
                pass

            from coffea.processor import IterativeExecutor
            fn = cloudpickle.loads(builder_bytes)
            sig = inspect.signature(fn).parameters
            kwargs = {}
            if "executor" in sig:
                kwargs["executor"] = IterativeExecutor()
            if builder_params:
                for k, v in builder_params.items():
                    if k in sig:
                        kwargs[k] = v
            return cloudpickle.dumps(fn(chunk_fileset, **kwargs))

        client = coffea_exec.client
        fn = _load_object(art.builder)
        builder_bytes = cloudpickle.dumps(fn)
        builder_params = dict(art.builder_params)

        # Build chunk artifacts, separate cached from uncached
        chunk_arts = []
        for entry in chunks_entries:
            chunk_arts.append(ChunkAnalysis(
                chunk_file=entry["file"],
                chunk_hash=entry["hash"],
                chunking=chunking,
                analysis_builder=art.builder,
                builder_params=art.builder_params,
            ))

        uncached_indices = [
            i for i, ca in enumerate(chunk_arts)
            if not deps._executor.exists(ca, config=config)
        ]

        if uncached_indices:
            _safe_print(f"Submitting {len(uncached_indices)} chunks in parallel...")
            futures = {}
            for i in uncached_indices:
                ca = chunk_arts[i]
                chunk_fileset = json.loads((chunk_dir / ca.chunk_file).read_text())
                futures[i] = client.submit(_run_chunk_remote, chunk_fileset, builder_bytes, builder_params)

            # Client.gather has no asyncio-style return_exceptions; collect
            # per-future so a failed chunk yields its exception in place and
            # results stay aligned with chunk indices.
            gathered = []
            for f in futures.values():
                try:
                    gathered.append(f.result())
                except Exception as exc:
                    gathered.append(exc)
                    
            for idx, result_or_exc in zip(futures.keys(), gathered):
                ca = chunk_arts[idx]
                out_dir = deps._executor.path_for(ca)
                out_dir.mkdir(parents=True, exist_ok=True)
                if isinstance(result_or_exc, BaseException):
                    _exc = result_or_exc
                    class _ExcResult:
                        def is_ok(self): return False
                        def __str__(self): return f"Worker exception: {_exc}"
                    payload = cloudpickle.dumps(_ExcResult())
                else:
                    payload = result_or_exc
                (out_dir / "payload.pkl").write_bytes(payload)
                _r = cloudpickle.loads(payload)
                if _r.is_ok():
                    (out_dir / ".success").touch()
                deps._executor._session_cache.add(out_dir)

        for i, (entry, ca) in enumerate(zip(chunks_entries, chunk_arts)):
            chunk_file = entry["file"]
            chunk_out_dir = deps._executor.path_for(ca)
            _safe_print("------------------------------------")
            _safe_print(f"Processing {chunk_file}")
            result = cloudpickle.loads((chunk_out_dir / "payload.pkl").read_bytes())
            if result.is_ok():
                _safe_print("Successfully processed!")
                acc, metrics = _extract_acc(result)
                merged_acc = accumulate([acc], accum=merged_acc)
                metrics_merged = accumulate([metrics], accum=metrics_merged)
            else:
                _safe_print("Failure caught!")
                failures.append({"chunk_file": chunk_file, "error": str(result)})
    else:
        for entry in chunks_entries:
            chunk_file = entry["file"]
            chunk_hash = entry["hash"]
            _safe_print("------------------------------------")
            _safe_print(f"Processing {chunk_file}")
            chunk_art = ChunkAnalysis(
                chunk_file=chunk_file,
                chunk_hash=chunk_hash,
                chunking=chunking,
                analysis_builder=art.builder,
                builder_params=art.builder_params,
            )
            # process chunk
            chunk_out_dir = deps.need(chunk_art)
            result = cloudpickle.loads((chunk_out_dir / "payload.pkl").read_bytes())
    
            #TODO: if config contains histserv_connection_info, then use the connection and add to the hist server, otherwise 
            if result.is_ok():
                _safe_print("Successfully processed!")
                acc, metrics = _extract_acc(result)
                if config.hist_client is not None:
                    # acc is already connection_info (returned directly from run_analysis)
                    # passing remote_hist directly is not possible because it holds a live gRPC connection, which is not picklable
                    merged_acc = config.histserv_connection_info # connection info to histserv
                else:
                    merged_acc = accumulate([acc], accum=merged_acc) # accumulatable
                metrics_merged = accumulate([metrics], accum=metrics_merged)
            else:
                _safe_print("Failure caught!")
                failures.append({"chunk_file": chunk_file, "error": str(result)})
                continue

    payload = {
        "builder": _builder_key(art.builder),
        "n_chunks_total": len(chunks_entries),
        "n_chunks_ok": 0 if merged_acc is None else (len(chunks_entries) - len(failures)),
        "failures": failures,
        "processor_result": (merged_acc, metrics_merged),
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "payload.pkl").write_bytes(cloudpickle.dumps(payload))
    (out / ".chunk_fraction").write_text(str(config.chunk_fraction))
    if failures:
        (out / ".has_failures").touch()
    else:
        (out / ".has_failures").unlink(missing_ok=True)


@producer(Plotting)
def make_plot(*, art: Plotting, deps: Deps, out: Path, config: RunConfig) -> None:
    out.mkdir(parents=True, exist_ok=True)
    analysis_dir = deps.need(art.analysis)
    payload = cloudpickle.loads((analysis_dir / "payload.pkl").read_bytes())
    fn = _load_object(art.builder)
    if config.histserv_connection_info is not None:
        plot_result = _call_builder(fn, config=config, builder_params=dict(art.builder_params))
    else:
        plot_result = _call_builder(fn, payload, builder_params=dict(art.builder_params))
    (out / "payload.pkl").write_bytes(cloudpickle.dumps(plot_result))


@producer(CustomArtifact)
def run_custom(*, art: CustomArtifact, deps: Deps, out: Path, config: RunConfig) -> None:
    out.mkdir(parents=True, exist_ok=True)

    upstream_results = []
    for upstream_art in art.upstreams:
        upstream_path = deps.need(upstream_art)
        upstream_results.append(_load_artifact_output(upstream_art, upstream_path))

    fn = _load_object(art.builder)
    result = _call_builder(fn, upstream_results, out=out, config=config,
                           builder_params=dict(art.builder_params))
    (out / "payload.pkl").write_bytes(cloudpickle.dumps(result))