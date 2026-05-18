from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

SplitStrategy = Literal["by_dataset"] | None

@dataclass(frozen=True)
class ExecutorConfig:
    """
    Controls which coffea executor the workflow injects into the analysis builder.

    Allows to options:
        1) to use pre-defined executor types like "FuturesExecutor", "DaskExecutor", "IterativeExecutor":
           ExecutorConfig(executor_type="FuturesExecutor", workers=8)
        2) set your own executor:
           ExecutorConfig(executor=processor.DaskExecutor(client=my_client))
    """
    executor_type: Literal["IterativeExecutor", "FuturesExecutor", "DaskExecutor"] = "FuturesExecutor"
    workers: int = 1
    chunks_per_worker: int = 1
    dask_scheduler: str | None = None
    executor: Any | None = None

    def __post_init__(self):
        if self.executor is not None:
            return
        if self.executor_type not in ("IterativeExecutor", "FuturesExecutor", "DaskExecutor"):
            raise ValueError(f"Invalid executor_type={self.executor_type!r}. Supported types are IterativeExecutor, FuturesExecutor, DaskExecutor")
        if self.executor_type == "DaskExecutor" and self.dask_scheduler is None:
            # TODO: when facilities added if coffea-casa, then automatically map scheduler
            raise ValueError("dask_scheduler is required when executor_type='DaskExecutor'")
        if self.workers < 1:
            raise ValueError("workers must be >= 1")
        if self.chunks_per_worker < 1:
            raise ValueError("chunks_per_worker must be >= 1")

@dataclass(frozen=True)
class RunConfig:
    """
    Defines how to run the analysis:
        - strategy: "by_dataset" splits into one chunk per dataset; None keeps all datasets together
        - percentage: what percent of each dataset's files per chunk (e.g. 20 → 5 chunks); None = no file split
        - datasets: restrict to specific dataset names; accepts list (auto-converted to tuple) or None for all
        - cache_dir: where to put cached outputs
    """
    strategy: SplitStrategy = None
    percentage: int | None = None
    datasets: tuple[str, ...] | None = None
    chunk_fraction: float | None = None
    cache_dir: Path = Path(".cache")
    hist_client: Any | None = None
    histserv_connection_info: dict | None = None
    executor_config: ExecutorConfig | None = None

    def __post_init__(self):
        if self.strategy not in (None, "by_dataset"):
            raise ValueError(
                f"Invalid strategy={self.strategy!r}. Use 'by_dataset' or None."
            )

        if self.percentage is not None:
            if not isinstance(self.percentage, int):
                raise TypeError("percentage must be an int")
            if not (1 <= self.percentage <= 100):
                raise ValueError("percentage must be between 1 and 100")
            if 100 % self.percentage != 0:
                raise ValueError(
                    "percentage must divide 100 evenly (e.g. 10, 20, 25, 50)."
                )
            

        # auto-convert list → tuple for hashability
        if isinstance(self.datasets, list):
            object.__setattr__(self, "datasets", tuple(self.datasets))

        if self.chunk_fraction is not None:
            if not isinstance(self.chunk_fraction, float) or not (0.0 < self.chunk_fraction <= 1.0):
                raise ValueError("chunk_fraction must be a float in (0.0, 1.0]")
