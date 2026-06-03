from coffea_workflow import Step, Workflow, Fileset, Analysis, Plotting, RunConfig, ExecutorConfig, render, CustomArtifact
from coffea_workflow import facilities
from coffea_workflow.facilities import CoffeaCasaFactory
from analysis import get_fileset, run_analysis, plot_results, custom_function_remove_last_file

step_fileset = Step(
    name="Fileset",
    step_type=Fileset,
    builder=get_fileset,
    builder_params={"to_print": "\nTEST:\nparameter testing...\nSUCCESS!\n"}
)

step_custom_filtering = Step(
    name="FilesetFiltering",
    step_type=CustomArtifact,
    builder=custom_function_remove_last_file,
)

step_analysis = Step(
    name="SingleMuonAnalysis",
    step_type=Analysis,
    builder=run_analysis,
)

step_plotting = Step(
    name="PlottingMuonAnalysis",
    step_type=Plotting,
    builder="analysis:plot_results"
)

workflow = Workflow()
workflow.add(step_fileset)
workflow.add(step_analysis, depends_on=[step_fileset])
workflow.add(step_plotting, depends_on=[step_analysis])

config = RunConfig(
    strategy="by_dataset",
    percentage=10,
    chunk_fraction=0.7,
    facility=facilities.local,
    executor_config=ExecutorConfig(executor_type="FuturesExecutor"),
)

render(workflow, config)
