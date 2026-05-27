import inspect
import importlib
from typing import TYPE_CHECKING, Any

from coffea.dataset_tools.splitting import split_fileset as _split_fileset

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
        from dask.distributed import Client, PipInstall
        client=Client(addr)
        plugin = PipInstall(packages=["coffea@git+https://github.com/hooloobooroodkoo/coffea.git@processor_result_type",
                                     "coffea-workflow@git+https://github.com/hooloobooroodkoo/coffea-workflow.git@main"])
        client.register_plugin(plugin)
        return DaskExecutor(client=client)

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
        from dask.distributed import Client, PipInstall
        client=Client(addr)
        plugin = PipInstall(packages=["coffea@git+https://github.com/hooloobooroodkoo/coffea.git@processor_result_type",
                                     "coffea-workflow@git+https://github.com/hooloobooroodkoo/coffea-workflow.git@main"])
        client.register_plugin(plugin)
        return DaskExecutor(client=client)


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

