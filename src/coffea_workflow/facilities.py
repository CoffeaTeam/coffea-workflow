"""
Pre-built facility factories for common HEP computing facilities.

Usage:
    from coffea_workflow import facilities

    config = RunConfig(facility=facilities.coffea_casa)
    config = RunConfig(facility=facilities.local)

    # With a pre-built image:
    config = RunConfig(facility=facilities.LxplusFactory(
        worker_image="/afs/cern.ch/user/u/user/worker.sif",
        queue="longlunch",
        workers=10,
    ))

    # Without an image — running locally generates worker.def and deployment
    # instructions; copy the folder to lxplus and follow the printed steps:
    config = RunConfig(facility=facilities.LxplusFactory())

Each factory owns:
  - preflight(): checks prerequisites, raises RuntimeError with exact fix commands;
                 for LxplusFactory with no worker_image, runs the image-build wizard
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
from dataclasses import dataclass
from typing import Any, ClassVar
from pathlib import Path
import textwrap

from .config import FacilityBase, ExecutorConfig
from .producers_utils import _safe_print

# ---------------------------------------------------------------------------
# Container helpers
# ---------------------------------------------------------------------------

_DEFAULT_BASE_IMAGE = (
    "gitlab-registry.cern.ch/batch-team/dask-lxplus/lxdask-al9:latest"
)

# minimum coffea release with use_result_type support required by coffea-workflow
_DEFAULT_COFFEA_SOURCE = "coffea>=2026.7.0"
_DEFAULT_COFFEA_WORKFLOW_SOURCE = (
    "git+https://github.com/hooloobooroodkoo/coffea-workflow.git"
)


def generate_apptainer_def(
    output: str = "worker.def",
    base_image: str = _DEFAULT_BASE_IMAGE,
    coffea_source: str = _DEFAULT_COFFEA_SOURCE,
    coffea_workflow_source: str = _DEFAULT_COFFEA_WORKFLOW_SOURCE,
    extra_packages: tuple[str, ...] = (),
    _print_build_instructions: bool = True,
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
    _safe_print(f"{output!r} was created!")
    _safe_print()
    _safe_print("Default image used for the base:")
    _safe_print(f"  {_DEFAULT_BASE_IMAGE}")
    _safe_print()
    _safe_print("Default sources (change these if needed):")
    _safe_print(f"  coffea:           {coffea_source}")
    _safe_print(f"  coffea-workflow:  {coffea_workflow_source}")
    _safe_print()
    _safe_print("To inspect your current environment and pin specific versions:")
    _safe_print("  pip freeze | grep -E 'coffea|uproot|awkward|hist|vector|dask|correctionlib'")
    _safe_print("  Then pass them as: extra_packages=('uproot==5.x.y', 'awkward==2.x.y', ...)")
    _safe_print()
    if _print_build_instructions:
        _safe_print("Build instructions:")
        _safe_print(f"  1. scp {output} <username>@lxplus.cern.ch:~/{output}")
        _safe_print("  2. scp -r /path/to/your/analysis <username>@lxplus.cern.ch:~/analysis")
        _safe_print("  3. ssh <username>@lxplus.cern.ch")
        _safe_print("  4. condor_submit -interactive        # get a batch node, wait for shell")
        _safe_print(f"  5. cp ~/{output} .  &&  apptainer build --fakeroot {sif} {output}")
        _safe_print(f"  6. cp {sif} ~/{sif}   # ~/  is AFS — same from login and batch nodes; wait, it's slow")
        _safe_print()
        _safe_print("To run directly:")
        _safe_print("  voms-proxy-init --voms cms --valid 192:00")
        _safe_print(f"  apptainer exec ~/{sif} python ~/path/to/your_script.py")
        _safe_print()
        _safe_print("Or with coffea-workflow LxplusFactory:")
        _safe_print(f"  LxplusFactory(worker_image='~/{sif}', ...)")

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
            n = (ec.workers if ec and ec.workers is not None else None) or self.workers
            return FuturesExecutor(workers=n)

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
    default_executor_type: ClassVar[str] = "DaskExecutor"
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
            n = (ec.workers if ec and ec.workers is not None else None) or 4
            return FuturesExecutor(workers=n)

        if executor_type == "DaskExecutor":
            return self._build_dask(ec)

        raise ValueError(f"Unsupported executor_type: {executor_type!r}")

    def _build_dask(self, ec: ExecutorConfig | None) -> Any:
        _safe_print("Connecting to Dask scheduler...")
        from coffea.processor import DaskExecutor
        from dask.distributed import Client, PipInstall

        client = Client(self.scheduler_address)
        
        # Upload files before installing packages
        files = (ec.worker_files if ec else ()) or self.worker_files
        for f in files:
            try:
                client.upload_file(f, load=False)
                _safe_print(f"Uploaded {f} to workers")
            except IsADirectoryError:
                folder = Path(f)
                _safe_print(f"{folder.name}/ is a directory, zipping...")
                zip_path = shutil.make_archive(
                    str(folder.resolve()),
                    "zip",
                    root_dir=str(folder.parent.resolve()),
                    base_dir=folder.name,
                )
                # load=True is required for zips: Dask must add the zip itself to
                # sys.path so Python's zipimport can find the package inside it.
                client.upload_file(zip_path, load=True)
                _safe_print(f"Uploaded {folder.name}/ as {folder.name}.zip to workers")

        packages = list((ec.worker_packages if ec else ()) or self.worker_packages)
        if packages:
            client.register_plugin(PipInstall(packages=packages))
            _safe_print(f"Installing on workers: {packages}")


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

    Two-phase workflow:
      1. Run locally (no lxplus needed): preflight() generates worker.def and
         run_on_lxplus.sh, then prints exact scp + build + run commands.
      2. On lxplus: copy the folder, build the Apptainer image on a batch node
         via the printed instructions, then run `bash run_on_lxplus.sh`.

    If worker_image is not provided on lxplus, the factory looks for worker.sif
    in the current directory (built in step 2 above).

    Requires on lxplus:
      - dask_jobqueue installed  (pip install dask-jobqueue)
      - a valid VOMS proxy       (voms-proxy-init --voms cms --valid 192:00)
      - HTCondor on PATH         (available on lxplus nodes)
    """
    worker_image: str | None = None
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
            self._run_local_setup()
            raise SystemExit(0)

        # --- On lxplus: check prerequisites ---
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

        if self.worker_image is None:
            sif = Path("worker.sif")
            if not sif.exists():
                raise RuntimeError(
                    "No worker_image set and worker.sif not found in the current directory.\n"
                    "Run your script locally first — it will generate worker.def and\n"
                    "print exact instructions for building the image and running on lxplus."
                )
            self.worker_image = str(sif.resolve())

    def _run_local_setup(self) -> None:
        """Generate worker.def and a run script, then print lxplus deployment instructions."""
        import sys as _sys

        _safe_print()
        _safe_print("=" * 60)
        _safe_print("LxplusFactory: running locally — preparing lxplus deployment.")
        _safe_print("=" * 60)

        def_path = Path("worker.def")
        if def_path.exists():
            _safe_print(f"\nworker.def already exists — keeping it.")
        else:
            _safe_print("\nNo worker.def found. Creating one.")
            _safe_print("Tip: check what's in your current environment with:")
            _safe_print("  pip freeze | grep -E 'coffea|uproot|awkward|hist|vector|dask|correctionlib'")
            raw_pkgs = input(
                "Extra packages to install in the image "
                "(comma-separated, or Enter to skip): "
            ).strip()
            extra_packages = tuple(p.strip() for p in raw_pkgs.split(",") if p.strip())
            generate_apptainer_def(
                output="worker.def",
                extra_packages=extra_packages,
                _print_build_instructions=False,
            )
            _safe_print("worker.def created.")

        sif_name = Path(self.worker_image).name if self.worker_image else "worker.sif"
        entry_script = Path(_sys.argv[0]).name
        if not entry_script.endswith(".py"):
            entry_script = "run.py"

        lxplus_script = Path("run_on_lxplus.sh")
        lxplus_script.write_text(textwrap.dedent(f"""\
            #!/bin/bash
            set -e
            voms-proxy-init --voms cms --valid 192:00
            KRB5DIR=$(dirname ${{KRB5CCNAME#FILE:}})
            apptainer exec \\
              --bind /tmp \\
              --bind /etc/condor \\
              --bind "$KRB5DIR" \\
              --env KRB5CCNAME=$KRB5CCNAME \\
              --env X509_USER_PROXY=/tmp/x509up_u$(id -u) \\
              --env PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \\
              ./{sif_name} python3 {entry_script}
        """))
        lxplus_script.chmod(0o755)
        _safe_print("run_on_lxplus.sh created.")

        cwd = Path.cwd()
        folder = cwd.name

        _safe_print()
        _safe_print("=" * 60)
        _safe_print("Next steps:")
        _safe_print()
        _safe_print("1. Copy this folder to lxplus:")
        _safe_print(f"   scp -r {cwd} <username>@lxplus.cern.ch:~/{folder}")
        _safe_print()
        _safe_print("2. Build the Apptainer image on a batch node:")
        _safe_print("   ssh <username>@lxplus.cern.ch")
        _safe_print(f"   cd ~/{folder}")
        _safe_print("   condor_submit -interactive    # wait for the batch shell")
        _safe_print(f"   cd ~/{folder}               # batch starts in a scratch dir; AFS is shared")
        _safe_print(f"   apptainer build --fakeroot {sif_name} worker.def")
        _safe_print()
        _safe_print("3. Run the analysis:")
        _safe_print(f"   bash run_on_lxplus.sh")
        _safe_print()
        _safe_print(f"The built {sif_name} is saved in ~/{folder}/ on AFS — it persists")
        _safe_print("between sessions and is picked up automatically on subsequent runs.")
        _safe_print("Rebuild only if you change packages or update coffea/coffea-workflow.")
        _safe_print("=" * 60)

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
            n = (ec.workers if ec and ec.workers is not None else None) or self.workers
            return FuturesExecutor(workers=n)

        if executor_type == "DaskExecutor":
            return self._build_dask(ec)

        raise ValueError(f"Unsupported executor_type: {executor_type!r}")

    def _build_dask(self, ec: ExecutorConfig | None) -> Any:
        import sys

        if self.worker_image is None:
            raise RuntimeError(
                "worker_image is not set. Either call preflight() first so it is resolved "
                "from worker.sif, or pass worker_image= to LxplusFactory()."
            )

        from dask_jobqueue import HTCondorCluster
        from dask.distributed import Client
        from coffea.processor import DaskExecutor

        worker_image = os.path.expanduser(self.worker_image)
        env_extra = []
        if self.extra_pythonpath:
            expanded = ":".join(os.path.expanduser(p) for p in self.extra_pythonpath)
            env_extra.append(f"PYTHONPATH={expanded}")

        # dask-jobqueue defaults to sys.executable, which may point to a host
        # virtualenv (e.g. in /afs/) that doesn't exist on batch nodes.
        # If that happens, fail early with a clear message instead of submitting
        # jobs that immediately exit 127.
        python_bin = sys.executable
        if "/afs/" in python_bin or "lxplus-env" in python_bin:
            raise RuntimeError(
                f"sys.executable points to an AFS virtualenv: {python_bin}\n"
                "HTCondor workers would fail to start because that path is not\n"
                "available inside the Apptainer container on batch nodes.\n\n"
                "Fix: run the container with --cleanenv so the host virtualenv\n"
                "does not leak into the container's PATH. Your run_on_lxplus.sh\n"
                "should include --cleanenv in the apptainer exec call.\n"
                "Re-generate it by running your script locally once more."
            )

        cluster = HTCondorCluster(
            cores=self.cores,
            memory=self.memory,
            disk=self.disk,
            log_directory=self.log_directory,
            python=python_bin,
            scheduler_options={"dashboard_address": ":8787"},
            worker_extra_args=["--worker-port", "10000:10100"],
            job_extra_directives={
                "+SingularityImage": f'"{worker_image}"',
                "+JobFlavour": f'"{self.queue}"',
                "stream_output": "False",
                "stream_error": "False",
                "transfer_output_files": '""',
            },
        )
        n_workers = (ec.workers if ec and ec.workers is not None else None) or self.workers
        cluster.scale(n_workers)
        _safe_print(f"Submitted {n_workers} HTCondor jobs (queue={self.queue!r}, image={self.worker_image!r}).")
        _safe_print(f"Dashboard: {cluster.dashboard_link}")

        client = Client(cluster)
        self._cluster = cluster

        packages = list((ec.worker_packages if ec else ()) or self.worker_packages)
        if packages:
            from dask.distributed import PipInstall
            client.register_plugin(PipInstall(packages=packages))
            _safe_print(f"Installing on workers: {packages}")

        files = (ec.worker_files if ec else ()) or self.worker_files
        for f in files:
            client.upload_file(f)
            _safe_print(f"Uploaded {f} to workers")

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
