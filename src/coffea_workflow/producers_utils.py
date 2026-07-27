import inspect
import importlib
import sys
from typing import TYPE_CHECKING, Any

from coffea.dataset_tools.splitting import split_fileset as _split_fileset

if TYPE_CHECKING:
    from .config import FacilityBase, ExecutorConfig


def _safe_print(*args, **kwargs):
    """
    Print directly to the underlying stream, bypassing the Rich proxy on coffea-casa.

    The Rich proxy on coffea-casa buffers output and its flush() can recurse infinitely
    when triggered while the buffer is non-empty. Writing to rich_proxied_file bypasses
    this entirely, matching the fix already applied to _print_summary in render.py.
    """
    kwargs.setdefault("file", getattr(sys.stdout, "rich_proxied_file", sys.stdout))
    print(*args, **kwargs)


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
    return fn(*args, **kwargs)

def build_executor(ec: "ExecutorConfig | None", facility: "FacilityBase | None" = None):
    """
    Build a coffea executor, delegating entirely to the facility.
    """
    from .facilities import LocalFactory
    return (facility or LocalFactory()).build(ec)


def _validate_runner_params(runner_params: dict) -> None:
    """
    Declarative mode (Analysis.processor) always injects the executor and forces
    use_result_type=True itself, so the chunk-merge logic can rely on a coffea Result.
    Reject runner_params that would collide with those framework-controlled kwargs.
    """
    forbidden = {"use_result_type", "executor"} & set(runner_params or {})
    if forbidden:
        raise ValueError(
            f"runner_params may not set {sorted(forbidden)} — coffea-workflow controls "
            "these (use_result_type is always True; executor is injected from "
            "RunConfig.executor_config/facility)."
        )


def _run_declarative(processor_ref, processor_params: dict, runner_params: dict, chunk_fileset, executor):
    """
    Declarative-mode counterpart to a user's run_analysis(fileset, executor) function:
    build processor.Runner(executor=..., use_result_type=True, **runner_params) and call
    it with processor_ref(**processor_params). Framework, not the user, owns the Runner.
    """
    from coffea.processor import Runner
    processor_params = processor_params or {}
    runner_params = runner_params or {}
    _validate_runner_params(runner_params)
    proc_cls = _load_object(processor_ref)
    proc = proc_cls(**processor_params)
    runner = Runner(executor=executor, use_result_type=True, **runner_params)
    return runner(chunk_fileset, proc)


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

