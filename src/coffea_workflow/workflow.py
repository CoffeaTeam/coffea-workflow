# TODO: add automatic code version tracking for reproducibility.
# from:
# - git commit hash ?
# - package version ?
# - container/image tag ?
# - hash of builder source ?
# This should be perhaps Artifacts identity part
    
from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, List, Sequence, Tuple, Type
from .artifacts import _builder_key

if TYPE_CHECKING:
    from .config import FacilityConfig, ExecutorConfig

@dataclass(frozen=True)
class Step:
    """
    Defines how the analysis step should be executed.

    Per-step overrides (both optional, both default to the workflow-level RunConfig):
        facility — WHERE to run (coffea-casa, lxplus, local)
        executor_config — HOW to run (DaskExecutor, FuturesExecutor, IterativeExecutor)

    Analysis parameters (strategy, percentage, datasets, chunk_fraction) and cache_dir
    are always taken from the workflow-level RunConfig and cannot be overridden per step.

    For step_type=Analysis, set exactly one of:
        builder — 'module:function' (or callable) that drives its own coffea Runner
        processor — 'module:Class' (or class) naming a ProcessorABC subclass; the
            framework builds and calls the Runner (processor_params construct the
            Processor, runner_params pass through to Runner()). See Analysis's
            docstring in artifacts.py for details.
    Other step_types (Fileset, Plotting, CustomArtifact) only use builder/builder_params.

    For step_type=Preprocessed (event-level splitting; see artifacts.Preprocessed):
        step_size — events per WorkItem (required)
        treename — fallback TTree name for list-format files
        custom_builder — optional per-file metadata extractor: fn(uproot_file) -> dict
        aggregate_builder — optional cross-file hook: fn(workitems) run once after
            preprocessing (e.g. dataset-level sum-of-weights totals)
    """
    name: str
    step_type: Type
    builder: str | Callable | None = None
    builder_params: dict | None = None
    processor: "str | Callable | None" = None
    processor_params: dict | None = None
    runner_params: dict | None = None
    step_size: int | None = None
    treename: str | None = None
    custom_builder: "str | Callable | None" = None
    aggregate_builder: "str | Callable | None" = None
    facility: "FacilityConfig | None" = None
    executor_config: "ExecutorConfig | None" = None
    input:  str | None = None
    output: str | None = None

    def _resolved_input(self) -> str:
        return self.input if self.input is not None else getattr(self.step_type, "input_type", "any")

    def _resolved_output(self) -> str:
        return self.output if self.output is not None else getattr(self.step_type, "output_type", "any")


    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "step_type": self.step_type.__name__,
            "builder": _builder_key(self.builder) if self.builder is not None else None,
            "builder_params": dict(self.builder_params) if self.builder_params else None,
            "processor": _builder_key(self.processor) if self.processor is not None else None,
            "processor_params": dict(self.processor_params) if self.processor_params else None,
            "runner_params": dict(self.runner_params) if self.runner_params else None,
            "step_size": self.step_size,
            "treename": self.treename,
            "custom_builder": _builder_key(self.custom_builder) if self.custom_builder is not None else None,
            "aggregate_builder": _builder_key(self.aggregate_builder) if self.aggregate_builder is not None else None,
            "facility": self.facility.name if self.facility else None,
            "executor_config": self.executor_config.executor_type if self.executor_config else None,
            "input":  self._resolved_input(),
            "output": self._resolved_output(),
        }

@dataclass
class Workflow:
    """
    Represents workflow DAG (analysis steps and their dependencies)
    """
    steps: List[Step] = field(default_factory=list)
    edges: List[Tuple[int, int]] = field(default_factory=list)

    def add(self, step: Step, depends_on: Sequence[Step] = ()) -> Step:
        dep_idxs = [self.steps.index(d) for d in depends_on]

        step_in = step._resolved_input()
        if step_in not in ("any", "none"):
            # input_type may be a union like "fileset_dict|workitems"
            allowed = set(step_in.split("|"))
            for di in dep_idxs:
                dep = self.steps[di]
                dep_out = dep._resolved_output()
                if dep_out != "any" and dep_out not in allowed:
                    raise TypeError(
                        f"Step '{step.name}' ({step.step_type.__name__}) expects input "
                        f"'{step_in}', but '{dep.name}' ({dep.step_type.__name__}) "
                        f"produces '{dep_out}'. "
                        f"Check depends_on or override input/output on the Step."
                    )

        
        self.steps.append(step)
        step_idx = len(self.steps) - 1
        for di in dep_idxs:
            self.edges.append((di, step_idx))
        return step
