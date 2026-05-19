import inspect
import importlib
import math
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .config import FacilityConfig, ExecutorConfig


def _call_builder(fn, *args, config=None, out=None, builder_params=None, executor=None):
    """
    Call fn(*args), injecting config as a kwarg if the function accepts it.
    For example, user uses client histserv in analysis function.
    """
    kwargs = {}
    sig = inspect.signature(fn).parameters
    if config is not None and "config" in sig:
        kwargs["config"] = config
    if out is not None and "out" in sig:
        kwargs["out"] = out
    if executor is not None and "executor" in sig:
        kwargs["executor"] = executor
    if builder_params:
        for k, v in builder_params.items():
            if k in sig:
                kwargs[k] = v
        print(f"\nkwargs: {kwargs}")
    return fn(*args, **kwargs)


def build_executor(ec: "ExecutorConfig | None", facility: "FacilityConfig | None" = None):
    """
    Build a coffea executor from an ExecutorConfig and/or FacilityConfig.

    Priority rules:
    - ec.executor (raw instance)       → use directly, ignore everything else
    - ec.executor_type == "DaskExecutor" → scheduler address from facility first,
                                           ec.dask_scheduler as fallback
    - ec is None, facility given        → infer executor type from facility name
    - both None                         → return None (analysis fn manages its own executor)
    """
    from coffea.processor import IterativeExecutor, FuturesExecutor, DaskExecutor

    if ec is None and facility is None:
        return None

    if ec is None:
        if facility.name == "local":
            return FuturesExecutor(workers=facility.workers)
        addr = _require_scheduler_address(facility)
        from dask.distributed import Client
        return DaskExecutor(client=Client(addr))

    if ec.executor is not None:
        return ec.executor

    if ec.executor_type == "IterativeExecutor":
        return IterativeExecutor()

    if ec.executor_type == "FuturesExecutor":
        return FuturesExecutor(workers=ec.workers)

    if ec.executor_type == "DaskExecutor":
        if facility is not None:
            addr = _require_scheduler_address(facility)
        elif ec.dask_scheduler is not None:
            addr = ec.dask_scheduler
        else:
            raise ValueError(
                "DaskExecutor requires either a facility with a scheduler address "
                "or dask_scheduler set in ExecutorConfig"
            )
        from dask.distributed import Client
        return DaskExecutor(client=Client(addr))


def _require_scheduler_address(facility: "FacilityConfig") -> str:
    addr = facility.get_scheduler_address()
    if not addr:
        env = (
            "COFFEA_CASA_SCHEDULER" if facility.name == "coffea-casa"
            else "LXPLUS_DASK_SCHEDULER"
        )
        raise ValueError(
            f"Facility '{facility.name}': set scheduler_address or the {env} environment variable"
        )
    return addr


def _load_artifact_output(art, path):
    """
    Load the payload of any materialized artifact generically.
    """
    if art.type_name == "Fileset":
        import json
        return json.loads((path / "fileset.json").read_text())
    payload_path = path / "payload.pkl"
    if payload_path.exists():
        import cloudpickle
        return cloudpickle.loads(payload_path.read_bytes())
    return None
    
def _extract_acc(result) -> Any:
    """
    Depending on the processor implementation, the user can return the accumulator or something else.
    Unwrap a Result, handling both savemetrics=True (tuple) and False (bare acc).
    """
    value = result.unwrap()
    if isinstance(value, tuple):
        acc, _metrics = value
        return acc, _metrics
    return value, {}
    
def _load_object(path: str | Any) -> Any:
    """
    Finds the function implemented by a user and returns it.
    Accepts either a 'module:function' string or a callable directly.
    """
    if callable(path):
        return path
    if ":" in path:
        mod_name, attr = path.split(":", 1)
    else:
        mod_name, attr = path.rsplit(".", 1)
    module = importlib.import_module(mod_name)
    try:
        return getattr(module, attr)
    except AttributeError as e:
        raise AttributeError(f"Object '{attr}' not found in module '{mod_name}'") from e


def _split_fileset(fileset, strategy=None, datasets=None, percentage=None):
    """
    Split the fileset into chunks to enable getting a partial result if one or several
    of the chunks failed to produce a result while being processed.
    One chunk is one partial fileset(unique combination of files), these are not usual coffea chunks.


    Input
    fileset:    {dataset: {"files": {path: treename, ...}}}
    strategy:   "by_dataset" — one dataset is one chunk; None — all datasets together
    percentage: integer that divides 100 evenly (20, 25, 50...).
                Each chunk gets this percentage of each dataset's files.
    datasets: list, callable or tuple of datasets' names

    Output
    List of fileset dicts
    If strategy only:
        chunks = _split_fileset(fileset, "by_dataset") - one chunk per dataset
    If percentage only:
        chunks = _split_fileset(fileset, percentage=50) - 2 chunks (50 of each dataset in 1st chunk and 2nd, mixed chunks
    If strategy and percentage:
        chunks = _split_fileset(fileset, "by_dataset", percentage=50) - N_datasets * 2 chunks, not mixed chunks
    If datasets + any/nothing:
        strategies are only applied to chosen datasets
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
        fileset = {k: v for k, v in fileset.items() if datasets(k)}
    else:
        fileset = {k: fileset[k] for k in datasets if k in fileset}

    if strategy == "by_dataset":
        groups = [{name: data} for name, data in fileset.items()]
    else:
        groups = [fileset]

    if percentage is None:
        return groups

    n_chunks = 100 // percentage
    result = []
    for group in groups:
        for bin_idx in range(n_chunks):
            chunk = {}
            for dataset, data in group.items():
                files = data.get("files", {})
                if not files:
                    continue
                
                if isinstance(files, dict):
                    file_items = list(files.items())
                    as_dict = True
                else:
                    file_items = list(files)
                    as_dict = False

                n = len(file_items)
                chunk_size = max(1, math.ceil(n / n_chunks))
                start = bin_idx * chunk_size
                end = min(start + chunk_size, n)
                if start >= n:
                    continue    
                sliced = file_items[start:end]
                chunk[dataset] = {**data, "files": dict(sliced) if as_dict else list(sliced)}
                
            if chunk:
                result.append(chunk)
    return result