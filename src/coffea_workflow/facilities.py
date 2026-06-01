"""
Pre-built facility factories for common HEP computing facilities.

Usage:
    from coffea_workflow import facilities

    config = RunConfig(facility=facilities.coffea_casa)
    config = RunConfig(facility=facilities.local)
    config = RunConfig(facility=facilities.LxplusFactory(
        worker_image="/cvmfs/.../coffea-dask.sif",
        queue="longlunch",
        workers=10,
    ))

Each factory owns:
  - preflight(): checks prerequisites, raises RuntimeError with exact fix commands
  - build(ec):   creates and returns a coffea executor
  - close():     tears down any created resources (e.g. Dask cluster)
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import importlib.util
import warnings
from dataclasses import dataclass
from typing import Any

from .config import FacilityBase, ExecutorConfig


# ---------------------------------------------------------------------------
# LocalFactory
# ---------------------------------------------------------------------------

@dataclass
class LocalFactory(FacilityBase):
    """
    Default facility factory if facility is not provided by a user
    
    Default executor if not provided by a user is FuturesExecutor.
    
    Runs on the local machine. Supports IterativeExecutor and FuturesExecutor.
    DaskExecutor requires an explicit scheduler_address.
    """
    workers: int = 4
    scheduler_address: str | None = None

    def build(self, ec: ExecutorConfig | None) -> Any:
        from coffea.processor import IterativeExecutor, FuturesExecutor, DaskExecutor

        if ec is not None and ec.executor is not None:
            return ec.executor

        executor_type = ec.executor_type if ec is not None else "FuturesExecutor"

        if executor_type == "IterativeExecutor":
            return IterativeExecutor()

        if executor_type == "FuturesExecutor":
            return FuturesExecutor(workers=ec.workers if ec else self.workers)

        if executor_type == "DaskExecutor":
            addr = (ec.dask_scheduler if ec else None) or self.scheduler_address
            if not addr:
                raise ValueError(
                    "LocalFactory with DaskExecutor requires a scheduler address.\n"
                    "Set scheduler_address= on LocalFactory or dask_scheduler= on ExecutorConfig."
                )
            from dask.distributed import Client
            client = Client(addr)
            return DaskExecutor(client=client)

        raise ValueError(f"Unsupported executor_type: {executor_type!r}")


# ---------------------------------------------------------------------------
# CoffeaCasaFactory
# ---------------------------------------------------------------------------

@dataclass
class CoffeaCasaFactory(FacilityBase):
    """
    CoffeaCasa facility.

    Default executor if not provided by a user is DaskExecutor.

    For DaskExecutor (default): connects to the pre-configured Dask scheduler
    at tls://localhost:8786. Other executor types are created directly.
    # TODO: optimised ways to run the analysis? optimised number of batches? split_strategy?
    """
    scheduler_address: str = "tls://localhost:8786"
    worker_packages: tuple[str, ...] = ()
    worker_files: tuple[str, ...] = ()

    def __post_init__(self):
        self.worker_packages = tuple(self.worker_packages)
        self.worker_files = tuple(self.worker_files)

    def build(self, ec: ExecutorConfig | None) -> Any:
        from coffea.processor import IterativeExecutor, FuturesExecutor, DaskExecutor

        if ec is not None and ec.executor is not None:
            return ec.executor

        executor_type = ec.executor_type if ec is not None else "DaskExecutor"

        if executor_type == "IterativeExecutor":
            return IterativeExecutor()

        if executor_type == "FuturesExecutor":
            return FuturesExecutor(workers=ec.workers if ec else 4)

        if executor_type == "DaskExecutor":
            return self._build_dask(ec)

        raise ValueError(f"Unsupported executor_type: {executor_type!r}")

    def _build_dask(self, ec: ExecutorConfig | None) -> Any:
        print("Connecting to Dask scheduler...")
        from coffea.processor import DaskExecutor
        from dask.distributed import Client, PipInstall

        client = Client(self.scheduler_address)

        packages = list((ec.worker_packages if ec else ()) or self.worker_packages)
        if packages:
            client.register_plugin(PipInstall(packages=packages))
            print(f"Installing on workers: {packages}")

        files = (ec.worker_files if ec else ()) or self.worker_files
        for f in files:
            client.upload_file(f)
            print(f"Uploaded {f} to workers")

        return DaskExecutor(client=client)


# ---------------------------------------------------------------------------
# Pre-built instances
# ---------------------------------------------------------------------------

local = LocalFactory()
coffea_casa = CoffeaCasaFactory()
