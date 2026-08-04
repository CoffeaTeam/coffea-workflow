import numpy as np
import hist
import histserv 
import coffea.processor as processor
import awkward as ak
from coffea.processor import Ok
from coffea.nanoevents import schemas
import grpc

def get_fileset():
    fileset = {'SingleMu_0':
           {"files":  {
               # broken link
                "root://eeeeeeospublic.cern.ch//eos/opendata/cms/mc/RunIISummer20UL16NanoAODv9/GluGluHToWWTo2L2Nu_M125_TuneCP5_13TeV_powheg2_JHUGenV714_pythia8/NANOAODSIM/106X_mcRun2_asymptotic_v17-v2/260000/A41320F6-C9F9-574C-8DD2-BD98C200E4EE.root": "Events",
                "root://eospublic.cern.ch//eos/opendata/cms/mc/RunIISummer20UL16NanoAODv9/GluGluHToWWTo2L2Nu_M125_TuneCP5_13TeV_powheg2_JHUGenV714_pythia8/NANOAODSIM/106X_mcRun2_asymptotic_v17-v2/260000/A7FEFB1C-387F-2B4D-A111-C53CC9371EC7.root": "Events",
                "root://eospublic.cern.ch//eos/opendata/cms/mc/RunIISummer20UL16NanoAODv9/GluGluHToWWTo2L2Nu_M125_TuneCP5_13TeV_powheg2_JHUGenV714_pythia8/NANOAODSIM/106X_mcRun2_asymptotic_v17-v2/260000/AB10FBAB-92C0-C043-933D-117FCC5704BA.root": "Events",
                "root://eospublic.cern.ch//eos/opendata/cms/mc/RunIISummer20UL16NanoAODv9/GluGluHToWWTo2L2Nu_M125_TuneCP5_13TeV_powheg2_JHUGenV714_pythia8/NANOAODSIM/106X_mcRun2_asymptotic_v17-v2/260000/C6E8BB7F-7F54-0C4C-9EDF-479C7DBB12E4.root": "Events",
                "root://eospublic.cern.ch//eos/opendata/cms/mc/RunIISummer20UL16NanoAODv9/GluGluHToWWTo2L2Nu_M125_TuneCP5_13TeV_powheg2_JHUGenV714_pythia8/NANOAODSIM/106X_mcRun2_asymptotic_v17-v2/260000/CB90AA65-868A-F548-A291-3837A3113162.root": "Events",
                }
           },
           'SingleMu_1':
           {"files":  {
                "root://eospublic.cern.ch//eos/opendata/cms/mc/RunIISummer20UL16NanoAODv9/GluGluHToWWTo2L2Nu_M125_TuneCP5_13TeV_powheg2_JHUGenV714_pythia8/NANOAODSIM/106X_mcRun2_asymptotic_v17-v2/260000/18B53494-657F-5744-8131-58ABA4EE00ED.root": "Events",
                "root://eospublic.cern.ch//eos/opendata/cms/mc/RunIISummer20UL16NanoAODv9/GluGluHToWWTo2L2Nu_M125_TuneCP5_13TeV_powheg2_JHUGenV714_pythia8/NANOAODSIM/106X_mcRun2_asymptotic_v17-v2/260000/2CCE1139-F301-C341-AE1E-4D27AF294018.root": "Events",
                "root://eospublic.cern.ch//eos/opendata/cms/mc/RunIISummer20UL16NanoAODv9/GluGluHToWWTo2L2Nu_M125_TuneCP5_13TeV_powheg2_JHUGenV714_pythia8/NANOAODSIM/106X_mcRun2_asymptotic_v17-v2/260000/300C603C-F1DD-4A40-B4DD-F4E0B239A460.root": "Events",
                "root://eospublic.cern.ch//eos/opendata/cms/mc/RunIISummer20UL16NanoAODv9/GluGluHToWWTo2L2Nu_M125_TuneCP5_13TeV_powheg2_JHUGenV714_pythia8/NANOAODSIM/106X_mcRun2_asymptotic_v17-v2/260000/39251543-EE21-9C4C-80D5-5D9178F55C71.root": "Events",
                "root://eospublic.cern.ch//eos/opendata/cms/mc/RunIISummer20UL16NanoAODv9/GluGluHToWWTo2L2Nu_M125_TuneCP5_13TeV_powheg2_JHUGenV714_pythia8/NANOAODSIM/106X_mcRun2_asymptotic_v17-v2/260000/3BD29D89-9C4A-E743-8616-C6806281BF12.root": "Events",
            }
           }
          }
    return fileset

class Processor(processor.ProcessorABC):
    # CHANGED
    def __init__(self, remote_hist):
    
        self.remote_hist = remote_hist
        self.output = processor.dict_accumulator({
            'cutflow': processor.defaultdict_accumulator(int)
        })

    
    def process(self, events):
        dataset = events.metadata["dataset"]
        MET = events.MET.pt
        
        self.output['cutflow']['all events'] += len(MET)
        self.output['cutflow']['number of chunks'] += 1

        # CHANGED
        try:
            self.remote_hist.fill(dataset=dataset, MET=MET)
        except grpc.RpcError as exc:
            raise RuntimeError(
                f"RPC failed with status {exc.code()}: {exc.details()}"
            ) from exc

        return self.output
    
    def postprocess(self, accumulator):
        pass

def hist_template():
    dataset_axis = hist.axis.StrCategory(name="dataset", label="", categories=[], growth=True)
    MET_axis = hist.axis.Regular(name="MET", label="MET [GeV]", bins=50, start=0, stop=100)
    return hist.Hist(dataset_axis, MET_axis)

# Analysis function should accept config parameter to be able to read histserv configs
def run_analysis(fileset, config, executor):
    # extract client and connection information
    hist_client = config.hist_client
    conn = config.histserv_connection_info
    print(f"conn: {conn}")

    # reconnect to the histogram 
    remote_hist = hist_client.connect(hist_id=conn["hist_id"], token=conn["token"])
    print(f"Reconnected to histserv: {remote_hist.get_connection_info()}")

    # run the processor
    executor_inst = executor
    run = processor.Runner(executor=executor_inst, schema=schemas.NanoAODSchema,
                           savemetrics=True, skipbadfiles=True, use_result_type=True)
    proc = Processor(remote_hist=remote_hist)
    result = run(fileset, proc)

    # return the histserv connection information to preserve the analysis result
    if result.is_ok():
        return Ok(remote_hist.get_connection_info())

    # return Err()
    return result

# accept config with histserv information
def plot_results(config):
    import matplotlib.pyplot as plt
    
    # reconnect to the histserv and retrieve information about the hist
    connection_info = config.histserv_connection_info
    hist_client = config.hist_client
    remote_hist = hist_client.connect(hist_id=connection_info["hist_id"], token=connection_info["token"])
    
    hist_result = remote_hist.snapshot().to_hist()
    
    # visualise the snapshot
    fig, ax = plt.subplots()
    for dataset in hist_result.axes[0]:
        hist_result[{"dataset": dataset}].plot1d(ax=ax, label=dataset)
    ax.set_xlabel("MET [GeV]")
    ax.set_ylabel("Events")
    ax.legend()
    plt.show()

