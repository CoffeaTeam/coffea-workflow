"""AGC ttbar with coffea-workflow on CERN lxplus (HTCondor + Apptainer workers).

Run this script once on any machine that is NOT lxplus: LxplusFactory.preflight()
then generates worker.def and run_on_lxplus.sh with the exact build and deploy
commands. See README.md in this folder for the full three-step walkthrough.
"""

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # lxplus is headless

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import matplotlib.pyplot as plt

from coffea_workflow import Step, Workflow, Fileset, Analysis, Plotting, RunConfig, ExecutorConfig, run
from coffea_workflow.facilities import LxplusFactory
from ttbar_analysis import get_fileset, run_analysis, plotting_1

step_fileset = Step(
    name="Fileset_ttbar",
    step_type=Fileset,
    builder=get_fileset,
    builder_params={"with_failure": False,
                    "n_files_max_per_sample": 2},  # -1 for the full AGC
    output="fileset_dict",
)
step_analysis = Step(
    name="Analysis_ttbar",
    step_type=Analysis,
    builder=run_analysis,
    builder_params={"use_inference": False},       # ML inference needs xgboost + models/
    input="fileset_dict",
    output="analysis_payload",
)
step_plotting = Step(
    name="Plot_ttbar",
    step_type=Plotting,
    builder=plotting_1,
    input="analysis_payload",
)

workflow = Workflow()
workflow.add(step_fileset)
workflow.add(step_analysis, depends_on=[step_fileset])
workflow.add(step_plotting, depends_on=[step_analysis])

config = RunConfig(
    strategy="by_dataset",
    cache_dir=".cache_lxplus",
    facility=LxplusFactory(
        worker_image="~/worker.sif",  # Apptainer image, built once (see README)
        queue="espresso",            # HTCondor flavour: espresso=20m, longlunch=2h, workday=8h
        workers=10,                   # HTCondor jobs, one Dask worker each
        cores=1,
        memory="2GB",
    ),
    executor_config=ExecutorConfig(executor_type="DaskExecutor"),
)

if __name__ == "__main__":
    result = run(workflow, config)
    plt.savefig("ttbar_4j1b.png", dpi=150, bbox_inches="tight")
    print("plot saved to ttbar_4j1b.png")
