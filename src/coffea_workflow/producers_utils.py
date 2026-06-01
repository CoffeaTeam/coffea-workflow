import inspect
import importlib
from typing import TYPE_CHECKING, Any

from coffea.dataset_tools.splitting import split_fileset as _split_fileset

if TYPE_CHECKING:
    from .config import FacilityBase, ExecutorConfig


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

def build_executor(ec: "ExecutorConfig | None", facility: "FacilityBase | None" = None):
    """
    Build a coffea executor, delegating entirely to the facility.
    """
    from .facilities import LocalFactory
    return (facility or LocalFactory()).build(ec)


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

