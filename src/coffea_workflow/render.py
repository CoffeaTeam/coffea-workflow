import dataclasses
import json
import typing
import cloudpickle
from .config import RunConfig
from .workflow import Workflow, Step
from .artifacts import ArtifactBase, Fileset, Preprocessed, Analysis, Plotting, CustomArtifact
from pathlib import Path
from .executor import Executor
from .producers_utils import _safe_print


def _topo_order(num_steps, edges):
    outgoing = {i: [] for i in range(num_steps)}
    in_deg = {i: 0 for i in range(num_steps)}
    for src, dst in edges:
        outgoing[src].append(dst)
        in_deg[dst] += 1

    queue = [i for i in range(num_steps) if in_deg[i] == 0]
    order = []
    while queue:
        idx = queue.pop(0)
        order.append(idx)
        for nxt in outgoing[idx]:
            in_deg[nxt] -= 1
            if in_deg[nxt] == 0:
                queue.append(nxt)

    if len(order) != num_steps:
        raise ValueError("Workflow has a cycle or disconnected dependency graph")
    return order


_PASSTHROUGH_FIELDS = (
    "builder", "builder_params", "processor", "processor_params", "runner_params",
    "step_size", "treename", "custom_builder", "aggregate_builder",
)


def _build_artifact(step_type, name, step: Step, upstream):
    """
    As I wanted user to only have to define name and builder/processor (from the Step
    values), but artifacts can require some specific parameter which are results of
    execution of the previous dependencies. These parameters will be filled by finding
    the matching artifact in upstream by type.

    Example of work:

    _build_artifact(Analysis, "SingleMuonAnalysis", step, upstream=[<Fileset artifact from step 1>])

    get_type_hints(Analysis) returns:
        {"name": str, "fileset": Fileset, "builder": str, "processor": str, ...}
    name -> skip
    fileset -> Fileset is a subclass of ArtifactBase → scan upstream → finds the Fileset artifact → kwargs["fileset"] = <that artifact>
    builder/builder_params/processor/processor_params/runner_params -> passed through from step
    when the target artifact type declares that field (e.g. Analysis has processor*, Fileset doesn't)

    """
    hints = typing.get_type_hints(step_type)
    kwargs = {"name": name}
    for field_name in _PASSTHROUGH_FIELDS:
        if field_name in hints:
            kwargs[field_name] = getattr(step, field_name)
    for field_name, field_type in hints.items():
        if field_name in ("name", *_PASSTHROUGH_FIELDS):
            continue
        if field_name == "upstreams":          # injecting all upstream artifacts if it's a custom artifact
            kwargs["upstreams"] = tuple(upstream)
            continue
        if isinstance(field_type, type) and issubclass(field_type, ArtifactBase):
            match = next((a for a in upstream if isinstance(a, field_type)), None)
            if match is None:
                raise RuntimeError(
                    f"{step_type.__name__} requires a '{field_name}' dependency of type "
                    f"{field_type.__name__}, but none was found in upstream steps."
                )
            kwargs[field_name] = match
    return step_type(**kwargs)


def _load_step_result(step_type, path: Path):
    if step_type is Fileset:
        return json.loads((path / "fileset.json").read_text())
    if step_type is Preprocessed:
        return json.loads((path / "workitems.json").read_text())
    if step_type is Analysis:
        return cloudpickle.loads((path / "payload.pkl").read_bytes())
    if step_type is Plotting:
        payload_path = path / "payload.pkl"
        return cloudpickle.loads(payload_path.read_bytes()) if payload_path.exists() else None
    if step_type is CustomArtifact:
        payload_path = path / "payload.pkl"
        return cloudpickle.loads(payload_path.read_bytes()) if payload_path.exists() else None
    return None



def _print_summary(step_results: dict) -> None:
    _safe_print("\n=== Run Summary ===")
    for name, (step_type, result) in step_results.items():
        if step_type is Analysis and result is not None:
            ok = result["n_chunks_ok"]
            total = result["n_chunks_total"]
            failures = result["failures"]
            marker = "✓" if not failures else "!"
            _safe_print(f"  {marker}  {name:<30} {step_type.__name__:<20} {ok}/{total} chunks OK")
            for f in failures:
                _safe_print(f"       FAILED {f['chunk_file']}: {f['error']}")
        else:
            _safe_print(f"  ✓  {name:<30} {step_type.__name__}")
    _safe_print()


def _print_dag(workflow: Workflow) -> None:
    _safe_print("Workflow DAG:")
    if not workflow.steps:
        _safe_print("  (no steps)")
        return
    for idx, step in enumerate(workflow.steps):
        if step.builder is not None:
            code_ref = f"builder={step.builder}"
        elif step.processor is not None:
            code_ref = f"processor={step.processor}"
        elif step.step_size is not None:
            code_ref = f"step_size={step.step_size}"
        else:
            code_ref = ""
        _safe_print(f"  [{idx}] {step.name} -> {step.step_type.__name__} {code_ref}")
    if workflow.edges:
        _safe_print("Edges:")
        for src, dst in workflow.edges:
            _safe_print(f"  {workflow.steps[src].name} -> {workflow.steps[dst].name}")
    else:
        _safe_print("Edges: (none)")


def _print_run_config(config: RunConfig) -> None:
    _safe_print("Run config:")
    _safe_print(f"  Strategy:  {config.strategy or 'none'}")

    ec = config.executor_config
    if ec is not None and ec.executor is None:
        _safe_print(f"  Executor:  {ec.executor_type}  workers={ec.workers}")
    elif ec is not None:
        _safe_print(f"  Executor:  custom ({type(ec.executor).__name__})")
    else:
        default = getattr(config.facility, "default_executor_type", "FuturesExecutor") \
                  if config.facility is not None else "FuturesExecutor"
        _safe_print(f"  Executor:  default ({default})")

    fac = config.facility
    if fac is None:
        _safe_print(f"  Facility:  local")
    else:
        fac_name = type(fac).__name__
        parts = []
        if hasattr(fac, "queue"):
            parts.append(f"queue={fac.queue!r}")
        if hasattr(fac, "workers"):
            parts.append(f"workers={fac.workers}")
        if hasattr(fac, "worker_image") and fac.worker_image:
            parts.append(f"image={fac.worker_image!r}")
        detail = f"  ({', '.join(parts)})" if parts else ""
        _safe_print(f"  Facility:  {fac_name}{detail}")


def _resolve_step_config(workflow_config: RunConfig, step: Step) -> RunConfig:
    """
    Merge per-step facility/executor overrides into the workflow config.

    Only facility and executor_config can be overridden per step.
    Analysis parameters (strategy, percentage, datasets, chunk_fraction) and
    cache_dir are always taken from the workflow-level RunConfig.
    """
    effective_facility = step.facility if step.facility is not None else workflow_config.facility
    effective_ec = step.executor_config if step.executor_config is not None else workflow_config.executor_config
    if effective_facility is workflow_config.facility and effective_ec is workflow_config.executor_config:
        return workflow_config
    return dataclasses.replace(
        workflow_config,
        facility=effective_facility,
        executor_config=effective_ec,
    )

def run(workflow: Workflow, config: RunConfig):
    """
    Executes the workflow DAG in topological order, materialising each artifact
    and returning cached results where available.
    """
    if config.facility is not None:
        config.facility.preflight()

    cache_dir = Path(config.cache_dir)
    executor = Executor(cache_dir=cache_dir, config=config)
    _print_dag(workflow)
    _safe_print()
    _print_run_config(config)
    _safe_print()
    num_steps = len(workflow.steps)
    if num_steps == 0:
        return {"paths": {}, "artifacts": {}, "order": []}

    order = _topo_order(num_steps, workflow.edges)

    parents: dict[int, list[int]] = {i: [] for i in range(num_steps)}
    for src, dst in workflow.edges:
        parents[dst].append(src)

    artifact_by_idx = {}
    paths_by_name = {}
    step_results = {}  # name -> (step_type, loaded result)

    try:
        for idx in order:
            step = workflow.steps[idx]
            step_name = step.name

            upstream = [artifact_by_idx[src] for src in parents[idx]]
            artifact = _build_artifact(step.step_type, step_name, step, upstream)

            effective_config = _resolve_step_config(config, step)
            if step.builder is not None:
                _safe_print(
                    f"Executing step '{step_name}' of type '{step.step_type.__name__}' with the user code {step.builder} and user parameters {step.builder_params}"
                )
            elif step.processor is not None:
                _safe_print(
                    f"Executing step '{step_name}' of type '{step.step_type.__name__}' with processor {step.processor} "
                    f"processor_params={step.processor_params} runner_params={step.runner_params}"
                )
            else:
                _safe_print(
                    f"Executing step '{step_name}' of type '{step.step_type.__name__}' "
                    f"(step_size={step.step_size}, custom_builder={step.custom_builder}, "
                    f"aggregate_builder={step.aggregate_builder})"
                )
            path = executor.materialize(artifact, config=effective_config)
            _safe_print(f"  -> materialized at {path}")
            _safe_print()

            artifact_by_idx[idx] = artifact
            paths_by_name[step_name] = path
            step_results[step_name] = (step.step_type, _load_step_result(step.step_type, path))

        _print_summary(step_results)
    finally:
        if config.facility is not None:
            config.facility.close()

    return {
        "paths": paths_by_name,
        "results": {name: result for name, (_, result) in step_results.items()},
        "order": [workflow.steps[i].name for i in order],
    }
