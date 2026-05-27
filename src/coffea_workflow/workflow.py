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
    """
    name: str
    step_type: Type
    builder: str | Callable
    builder_params: dict | None = None
    facility: "FacilityConfig | None" = None
    executor_config: "ExecutorConfig | None" = None

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "step_type": self.step_type.__name__,
            "builder": _builder_key(self.builder),
            "builder_params": dict(self.builder_params) if self.builder_params else None,
            "facility": self.facility.name if self.facility else None,
            "executor_config": self.executor_config.executor_type if self.executor_config else None,
        }

@dataclass
class Workflow:
    """
    Represents workflow DAG (analysis steps and their dependencies)
    """
    steps: List[Step] = field(default_factory=list)
    edges: List[Tuple[int, int]] = field(default_factory=list)

    def add(self, step: Step, depends_on: Sequence[Step] = ()) -> Step:
        self.steps.append(step)
        step_idx = len(self.steps) - 1
        dep_idxs = [self.steps.index(d) for d in depends_on]
        for di in dep_idxs:
            self.edges.append((di, step_idx))
        return step
