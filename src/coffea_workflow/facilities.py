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

Container helpers (for lxplus):
    facilities.generate_apptainer_def()              # write worker.def from user-defined env
    LxplusFactory(...).generate_apptainer_def()      # same, bound to the factory instance
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
from pathlib import Path
import textwrap

from .config import FacilityBase, ExecutorConfig

# ---------------------------------------------------------------------------
# Container helpers
# ---------------------------------------------------------------------------

_DEFAULT_BASE_IMAGE = (
    "gitlab-registry.cern.ch/batch-team/dask-lxplus/lxdask-al9:latest"
)

# the one that is mandatory to use as for now for coffea-workflow
_DEFAULT_COFFEA_SOURCE = (
    "git+https://github.com/hooloobooroodkoo/coffea.git@processor_result_type"
)
_DEFAULT_COFFEA_WORKFLOW_SOURCE = (
    "git+https://github.com/hooloobooroodkoo/coffea-workflow.git"
)


def generate_apptainer_def(
    output: str = "worker.def",
    base_image: str = _DEFAULT_BASE_IMAGE,
    coffea_source: str = _DEFAULT_COFFEA_SOURCE,
    coffea_workflow_source: str = _DEFAULT_COFFEA_WORKFLOW_SOURCE,
    extra_packages: tuple[str, ...] = (),
) -> str:
    """
    Write an Apptainer definition file for lxplus workers.

    Defaults to installing coffea and coffea-workflow from git. You are welcome
    to swap these for PyPI versions or your own forks. Base image can also be changed.

    To find what is currently installed in your environment and add it as
    extra_packages, run:
        pip freeze | grep -E 'coffea|uproot|awkward|hist|vector|dask|correctionlib'

    Args:
        output:                  path to write the .def file
        base_image:              Docker base (default: CERN batch team lxplus EL9 image,
                                 has XRootD and HTCondor/Dask pre-configured)
        coffea_source:           coffea install spec (git URL or "coffea==X.Y.Z")
        coffea_workflow_source:  coffea-workflow install spec (git URL or PyPI spec potentially in the future)
        extra_packages:          additional packages, e.g. ("xgboost", "correctionlib==2.1.0")

    Returns:
        path to the written .def file
    """
    packages = [coffea_source, coffea_workflow_source, *extra_packages]
    pkg_lines = " \\\n        ".join(f'"{p}"' for p in packages)
    post_body = f"    pip install --no-cache-dir \\\n        {pkg_lines}"

    # worker.def content
    content = textwrap.dedent(f"""\
        Bootstrap: docker
        From: {base_image}

        %post
        {post_body}

        %environment
            export PYTHONNOUSERSITE=1
    """)

    Path(output).write_text(content)

    sif = Path(output).with_suffix(".sif").name
    print(f"{output!r} was created!")
    print()
    print("Default image used for the base:")
    print(f"  {_DEFAULT_BASE_IMAGE}")
    print()
    print("Default sources (change these if needed):")
    print(f"  coffea:           {coffea_source}")
    print(f"  coffea-workflow:  {coffea_workflow_source}")
    print()
    print("To inspect your current environment and pin specific versions:")
    print("  pip freeze | grep -E 'coffea|uproot|awkward|hist|vector|dask|correctionlib'")
    print("  Then pass them as: extra_packages=('uproot==5.x.y', 'awkward==2.x.y', ...)")
    print()
    print("Build instructions:")
    print(f"  1. scp {output} <username>@lxplus.cern.ch:~/{output}")
    print("  2. scp -r /path/to/your/analysis <username>@lxplus.cern.ch:~/analysis")
    print("  3. ssh <username>@lxplus.cern.ch")
    print("  4. condor_submit -interactive        # get a batch node, wait for shell")
    print(f"  5. cp ~/{output} .  &&  apptainer build --fakeroot {sif} {output}")
    print(f"  6. cp {sif} ~/{sif}   # ~/  is AFS — same from login and batch nodes; wait, it's slow")
    print()
    print("To run directly:")
    print("  voms-proxy-init --voms cms --valid 192:00")
    print(f"  apptainer exec ~/{sif} python ~/path/to/your_script.py")
    print()
    print("Or with coffea-workflow LxplusFactory:")
    print(f"  LxplusFactory(worker_image='~/{sif}', ...)")

    return output


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
        
        # Upload files before installing packages
        files = (ec.worker_files if ec else ()) or self.worker_files
        for f in files:
            client.upload_file(f, load=False)
            print(f"Uploaded {f} to workers")

        packages = list((ec.worker_packages if ec else ()) or self.worker_packages)
        if packages:
            client.register_plugin(PipInstall(packages=packages))
            print(f"Installing on workers: {packages}")


        return DaskExecutor(client=client)

# ---------------------------------------------------------------------------
# LxplusFactory
# ---------------------------------------------------------------------------

@dataclass
class LxplusFactory(FacilityBase):
    """
    CERN lxplus facility.

    Submits Dask workers as HTCondor jobs via dask_jobqueue.HTCondorCluster,
    running inside the specified Singularity/Apptainer image on CVMFS.
    Default executor is FuturesExecutor.

    Requires:
      - dask_jobqueue installed  (pip install dask-jobqueue)
      - a valid VOMS proxy       (voms-proxy-init --voms cms --valid 192:00)
      - HTCondor on PATH         (available on lxplus nodes)
    """
    worker_image: str
    queue: str = "longlunch"
    workers: int = 10
    cores: int = 1
    memory: str = "2GB"
    disk: str = "1GB"
    log_directory: str = "logs"
    worker_packages: tuple[str, ...] = ()
    worker_files: tuple[str, ...] = ()
    extra_pythonpath: tuple[str, ...] = () #this one added for developing stage to modify coffea-workflow in lxplus and use that package


    def __post_init__(self):
        self.worker_packages = tuple(self.worker_packages)
        self.worker_files = tuple(self.worker_files)
        self.extra_pythonpath = tuple(self.extra_pythonpath)
        self._cluster = None

    def preflight(self) -> None:
        hostname = socket.gethostname()
        if "lxplus" not in hostname:
            warnings.warn(
                f"LxplusFactory: running on {hostname!r}, expected an lxplus node. "
                "HTCondor job submission may fail.",
                UserWarning,
                stacklevel=2,
            )

        if shutil.which("condor_q") is None:
            raise RuntimeError(
                "HTCondor is not available on PATH. "
                "Run on an lxplus node: https://batchdocs.web.cern.ch/local/submit.html"
            )

        if importlib.util.find_spec("dask_jobqueue") is None:
            raise RuntimeError(
                "dask_jobqueue is not installed. Install it with:\n"
                "  pip install dask-jobqueue"
            )

        try:
            result = subprocess.run(
                ["voms-proxy-info", "--timeleft"],
                capture_output=True,
                text=True,
            )
        except FileNotFoundError:
            raise RuntimeError(
                "voms-proxy-info not found. Ensure VOMS client tools are installed "
                "and create a proxy:\n"
                "  voms-proxy-init --voms cms --valid 192:00"
            )
        timeleft = 0
        try:
            timeleft = int(result.stdout.strip())
        except ValueError:
            pass
        if result.returncode != 0 or timeleft <= 0:
            raise RuntimeError(
                "No valid VOMS proxy found or proxy has expired. Create one with:\n"
                "  voms-proxy-init --voms cms --valid 192:00"
            )

    def build(self, ec: ExecutorConfig | None) -> Any:
        import sys
        from coffea.processor import IterativeExecutor, FuturesExecutor, DaskExecutor

        if self.extra_pythonpath:
            for p in reversed(self.extra_pythonpath):
                expanded = os.path.expanduser(p)
                if expanded not in sys.path:
                    sys.path.insert(0, expanded)

        if ec is not None and ec.executor is not None:
            return ec.executor

        executor_type = ec.executor_type if ec is not None else "FuturesExecutor"

        if executor_type == "IterativeExecutor":
            return IterativeExecutor()

        if executor_type == "FuturesExecutor":
            return FuturesExecutor(workers=ec.workers if ec else self.workers)

        if executor_type == "DaskExecutor":
            return self._build_dask(ec)

        raise ValueError(f"Unsupported executor_type: {executor_type!r}")

    def _build_dask(self, ec: ExecutorConfig | None) -> Any:
        from dask_jobqueue import HTCondorCluster
        from dask.distributed import Client
        from coffea.processor import DaskExecutor

        worker_image = os.path.expanduser(self.worker_image)
        env_extra = []
        if self.extra_pythonpath:
            expanded = ":".join(os.path.expanduser(p) for p in self.extra_pythonpath)
            env_extra.append(f"PYTHONPATH={expanded}")
            
        cluster = HTCondorCluster(
            cores=self.cores,
            memory=self.memory,
            disk=self.disk,
            log_directory=self.log_directory,
            job_extra_directives={
                "+SingularityImage": f'"{worker_image}"',
                "+JobFlavour": f'"{self.queue}"',
            },
        )
        n_workers = (ec.workers if ec else None) or self.workers
        cluster.scale(n_workers)
        print(f"Submitted {n_workers} HTCondor jobs (queue={self.queue!r}, image={self.worker_image!r}).")
        print(f"Dashboard: {cluster.dashboard_link}")

        client = Client(cluster)
        self._cluster = cluster

        packages = list((ec.worker_packages if ec else ()) or self.worker_packages)
        if packages:
            from dask.distributed import PipInstall
            client.register_plugin(PipInstall(packages=packages))
            print(f"Installing on workers: {packages}")

        files = (ec.worker_files if ec else ()) or self.worker_files
        for f in files:
            client.upload_file(f)
            print(f"Uploaded {f} to workers")

        return DaskExecutor(client=client)

    def close(self) -> None:
        if self._cluster is not None:
            self._cluster.close()
            self._cluster = None

        
# ---------------------------------------------------------------------------
# Pre-built instances
# ---------------------------------------------------------------------------

local = LocalFactory()
coffea_casa = CoffeaCasaFactory()
