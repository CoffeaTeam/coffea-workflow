"""
AGC cms ttbar example without ServiceX.
"""

import logging
import time

import awkward as ak
import cabinetry
import cloudpickle
import correctionlib
from coffea import processor
from coffea.nanoevents import NanoAODSchema
from coffea.analysis_tools import PackedSelection
import copy
import hist
import matplotlib.pyplot as plt
import numpy as np
import pyhf

import utils  # contains code for bookkeeping and cosmetics, as well as some boilerplate
import utils.config
import utils.metrics
import utils.plotting

logging.getLogger("cabinetry").setLevel(logging.INFO)

### GLOBAL CONFIGURATION
# input files per process, set to e.g. 10 (smaller number = faster)
N_FILES_MAX_PER_SAMPLE = 5

# enable Dask
USE_DASK = False

# enable ServiceX, specify options
USE_SERVICEX = False
USE_SERVICEX_UPROOT_RAW = True # set False to use func_adl instead
USE_SERVICEX_DOWNLOAD = False # set False to use remote data access

### ML-INFERENCE SETTINGS

# enable ML inference
USE_INFERENCE = True

# enable inference using NVIDIA Triton server
USE_TRITON = False


### FILESET
def get_fileset():
    fileset = utils.file_input.construct_fileset(
    N_FILES_MAX_PER_SAMPLE,
    use_xcache=False,
    af_name=utils.config["benchmarking"]["AF_NAME"],  # local files on /data for af_name="ssl-dev"
    input_from_eos=utils.config["benchmarking"]["INPUT_FROM_EOS"],
    xcache_atlas_prefix=utils.config["benchmarking"]["XCACHE_ATLAS_PREFIX"],
)
    print(f"processes in fileset: {list(fileset.keys())}")
    print(f"\nexample of information in fileset:\n{{\n  'files': [{fileset['ttbar__nominal']['files'][0]}, ...],")
    print(f"  'metadata': {fileset['ttbar__nominal']['metadata']}\n}}")
    return fileset



### ANALYSIS
class TtbarAnalysis(processor.ProcessorABC):
    def __init__(self, use_inference, use_triton):

        # initialize dictionary of hists for signal and control region
        self.hist_dict = {}
        for region in ["4j1b", "4j2b"]:
            self.hist_dict[region] = (
                hist.Hist.new.Reg(utils.config["global"]["NUM_BINS"], 
                                  utils.config["global"]["BIN_LOW"], 
                                  utils.config["global"]["BIN_HIGH"], 
                                  name="observable", 
                                  label="observable [GeV]")
                .StrCat([], name="process", label="Process", growth=True)
                .StrCat([], name="variation", label="Systematic variation", growth=True)
                .Weight()
            )
        
        self.cset = correctionlib.CorrectionSet.from_file("corrections.json")
        self.use_inference = use_inference
        
        # set up attributes only needed if USE_INFERENCE=True
        if self.use_inference:
            
            # initialize dictionary of hists for ML observables
            self.ml_hist_dict = {}
            for i in range(len(utils.config["ml"]["FEATURE_NAMES"])):
                self.ml_hist_dict[utils.config["ml"]["FEATURE_NAMES"][i]] = (
                    hist.Hist.new.Reg(utils.config["global"]["NUM_BINS"],
                                      utils.config["ml"]["BIN_LOW"][i],
                                      utils.config["ml"]["BIN_HIGH"][i],
                                      name="observable",
                                      label=utils.config["ml"]["FEATURE_DESCRIPTIONS"][i])
                    .StrCat([], name="process", label="Process", growth=True)
                    .StrCat([], name="variation", label="Systematic variation", growth=True)
                    .Weight()
                )
            
            self.use_triton = use_triton
            if not use_triton:
                # Pre-load models as instance attributes so cloudpickle embeds them
                # in the serialized processor. Workers receive the models directly
                # without needing the XGBoost model files on disk.
                utils.ml.load_models()
                self.model_even = utils.ml.model_even
                self.model_odd = utils.ml.model_odd

    def only_do_IO(self, events):
        for branch in utils.config["benchmarking"]["IO_BRANCHES"][
            utils.config["benchmarking"]["IO_FILE_PERCENT"]
        ]:
            if "_" in branch:
                split = branch.split("_")
                object_type = split[0]
                property_name = "_".join(split[1:])
                ak.materialized(events[object_type][property_name])
            else:
                ak.materialized(events[branch])
        return {"hist": {}}

    def process(self, events):
        if utils.config["benchmarking"]["DISABLE_PROCESSING"]:
            # IO testing with no subsequent processing
            return self.only_do_IO(events)

        # create copies of histogram objects
        hist_dict = copy.deepcopy(self.hist_dict)
        if self.use_inference:
            ml_hist_dict = copy.deepcopy(self.ml_hist_dict)

        process = events.metadata["process"]  # "ttbar" etc.
        variation = events.metadata["variation"]  # "nominal" etc.

        # normalization for MC
        x_sec = events.metadata["xsec"]
        nevts_total = events.metadata["nevts"]
        lumi = 3378 # /pb
        if process != "data":
            xsec_weight = x_sec * lumi / nevts_total
        else:
            xsec_weight = 1
            
        # setup triton gRPC client (local models are already on self)
        if self.use_inference:
            if self.use_triton:
                triton_client = utils.clients.get_triton_client(utils.config["ml"]["TRITON_URL"])


        #### systematics
        # jet energy scale / resolution systematics
        # need to adjust schema to instead use coffea add_systematic feature, especially for ServiceX
        # cannot attach pT variations to events.jet, so attach to events directly
        # and subsequently scale pT by these scale factors
        events["pt_scale_up"] = 1.03
        events["pt_res_up"] = utils.systematics.jet_pt_resolution(events.Jet.pt)

        syst_variations = ["nominal"]
        jet_kinematic_systs = ["pt_scale_up", "pt_res_up"]
        event_systs = [f"btag_var_{i}" for i in range(4)]
        if process == "wjets":
            event_systs.append("scale_var")

        # Only do systematics for nominal samples, e.g. ttbar__nominal
        if variation == "nominal":
            syst_variations.extend(jet_kinematic_systs)
            syst_variations.extend(event_systs)

        # for pt_var in pt_variations:
        for syst_var in syst_variations:
            ### event selection
            # very very loosely based on https://arxiv.org/abs/2006.13076

            # Note: This creates new objects, distinct from those in the 'events' object
            elecs = events.Electron
            muons = events.Muon
            jets = events.Jet
            if syst_var in jet_kinematic_systs:
                # Replace jet.pt with the adjusted values
                jets["pt"] = jets.pt * events[syst_var]

            electron_reqs = (elecs.pt > 30) & (np.abs(elecs.eta) < 2.1) & (elecs.cutBased == 4) & (elecs.sip3d < 4)
            muon_reqs = ((muons.pt > 30) & (np.abs(muons.eta) < 2.1) & (muons.tightId) & (muons.sip3d < 4) &
                         (muons.pfRelIso04_all < 0.15))
            jet_reqs = (jets.pt > 30) & (np.abs(jets.eta) < 2.4) & (jets.isTightLeptonVeto)

            # Only keep objects that pass our requirements
            elecs = elecs[electron_reqs]
            muons = muons[muon_reqs]
            jets = jets[jet_reqs]

            if self.use_inference:
                even = (events.event%2==0)  # whether events are even/odd

            B_TAG_THRESHOLD = 0.5

            ######### Store boolean masks with PackedSelection ##########
            selections = PackedSelection(dtype='uint64')
            # Basic selection criteria
            selections.add("exactly_1l", (ak.num(elecs) + ak.num(muons)) == 1)
            selections.add("atleast_4j", ak.num(jets) >= 4)
            selections.add("exactly_1b", ak.sum(jets.btagCSVV2 > B_TAG_THRESHOLD, axis=1) == 1)
            selections.add("atleast_2b", ak.sum(jets.btagCSVV2 > B_TAG_THRESHOLD, axis=1) >= 2)
            # Complex selection criteria
            selections.add("4j1b", selections.all("exactly_1l", "atleast_4j", "exactly_1b"))
            selections.add("4j2b", selections.all("exactly_1l", "atleast_4j", "atleast_2b"))

            for region in ["4j1b", "4j2b"]:
                region_selection = selections.all(region)
                region_jets = jets[region_selection]
                region_elecs = elecs[region_selection]
                region_muons = muons[region_selection]
                region_weights = np.ones(len(region_jets)) * xsec_weight
                if self.use_inference:
                    region_even = even[region_selection]

                if region == "4j1b":
                    observable = ak.sum(region_jets.pt, axis=-1)

                elif region == "4j2b":

                    # reconstruct hadronic top as bjj system with largest pT
                    trijet = ak.combinations(region_jets, 3, fields=["j1", "j2", "j3"])  # trijet candidates
                    trijet["p4"] = trijet.j1 + trijet.j2 + trijet.j3  # calculate four-momentum of tri-jet system
                    trijet["max_btag"] = np.maximum(trijet.j1.btagCSVV2, np.maximum(trijet.j2.btagCSVV2, trijet.j3.btagCSVV2))
                    trijet = trijet[trijet.max_btag > B_TAG_THRESHOLD]  # at least one-btag in trijet candidates
                    # pick trijet candidate with largest pT and calculate mass of system
                    trijet_mass = trijet["p4"][ak.argmax(trijet.p4.pt, axis=1, keepdims=True)].mass
                    observable = ak.flatten(trijet_mass)

                    if sum(region_selection)==0:
                        continue

                    if self.use_inference:
                        features, perm_counts = utils.ml.get_features(
                            region_jets,
                            region_elecs,
                            region_muons,
                            max_n_jets=utils.config["ml"]["MAX_N_JETS"],
                        )
                        even_perm = np.repeat(region_even, perm_counts)

                        # calculate ml observable
                        if self.use_triton:
                            results = utils.ml.get_inference_results_triton(
                                features,
                                even_perm,
                                triton_client,
                                utils.config["ml"]["MODEL_NAME"],
                                utils.config["ml"]["MODEL_VERSION_EVEN"],
                                utils.config["ml"]["MODEL_VERSION_ODD"],
                            )
                        else:
                            results = utils.ml.get_inference_results_local(
                                features,
                                even_perm,
                                self.model_even,
                                self.model_odd,
                            )
                            
                        results = ak.unflatten(results, perm_counts)
                        features = ak.flatten(ak.unflatten(features, perm_counts)[
                            ak.from_regular(ak.argmax(results,axis=1)[:, np.newaxis])
                        ])
                syst_var_name = f"{syst_var}"
                # Break up the filling into event weight systematics and object variation systematics
                if syst_var in event_systs:
                    for i_dir, direction in enumerate(["up", "down"]):
                        # Should be an event weight systematic with an up/down variation
                        if syst_var.startswith("btag_var"):
                            i_jet = int(syst_var.rsplit("_",1)[-1])   # Kind of fragile
                            wgt_variation = self.cset["event_systematics"].evaluate("btag_var", direction, region_jets.pt[:,i_jet])
                        elif syst_var == "scale_var":
                            # The pt array is only used to make sure the output array has the correct shape
                            wgt_variation = self.cset["event_systematics"].evaluate("scale_var", direction, region_jets.pt[:,0])
                        syst_var_name = f"{syst_var}_{direction}"
                        hist_dict[region].fill(
                            observable=observable, process=process,
                            variation=syst_var_name, weight=region_weights * wgt_variation
                        )
                        if region == "4j2b" and self.use_inference:
                            for i in range(len(utils.config["ml"]["FEATURE_NAMES"])):
                                ml_hist_dict[utils.config["ml"]["FEATURE_NAMES"][i]].fill(
                                    observable=features[..., i], process=process,
                                    variation=syst_var_name, weight=region_weights * wgt_variation
                                )
                else:
                    # Should either be 'nominal' or an object variation systematic
                    if variation != "nominal":
                        # This is a 2-point systematic, e.g. ttbar__scaledown, ttbar__ME_var, etc.
                        syst_var_name = variation
                    hist_dict[region].fill(
                        observable=observable, process=process,
                        variation=syst_var_name, weight=region_weights
                    )
                    if region == "4j2b" and self.use_inference:
                        for i in range(len(utils.config["ml"]["FEATURE_NAMES"])):
                            ml_hist_dict[utils.config["ml"]["FEATURE_NAMES"][i]].fill(
                                observable=features[..., i], process=process,
                                variation=syst_var_name, weight=region_weights
                            )


        output = {"nevents": {events.metadata["dataset"]: len(events)}, "hist_dict": hist_dict}
        if self.use_inference:
            output["ml_hist_dict"] = ml_hist_dict

        return output

    def postprocess(self, accumulator):
        return accumulator

def run_analysis(fileset, executor=None): # <- CHANGED, do not forget to pass fileset
    NanoAODSchema.warn_missing_crossrefs = False # silences warnings about branches we will not use here
    if executor:
        executor=executor
    else:
        if USE_DASK:
            cloudpickle.register_pickle_by_value(utils) # serialize methods and objects in utils so that they can be accessed within the coffea processor
            # CHANGED TEMPORARILY
            executor = processor.DaskExecutor(client=utils.clients.get_client(af="coffea_dev")) # dask client for coffea-casa, install my dev coffea on workers
        else:
            executor = processor.FuturesExecutor(workers=utils.config["benchmarking"]["NUM_CORES"])
    
    run = processor.Runner(
                            executor=executor,
                            schema=NanoAODSchema,
                            savemetrics=True,
                            metadata_cache={},
                            chunksize=utils.config["benchmarking"]["CHUNKSIZE"],
                            skipbadfiles=True, # CHANGED
                            use_result_type=True, # CHANGED
                        )
    
    if USE_SERVICEX:
        treename = "servicex"
    else:
        treename = "Events"
    
    # load local models if not using Triton or FuturesExecutor and models are not yet loaded
    if USE_INFERENCE and not USE_TRITON and USE_DASK and utils.ml.model_even is None and utils.ml.model_odd is None:
        utils.ml.load_models()
    
    filemeta = run.preprocess(fileset, treename=treename)  # pre-processing
    
    t0 = time.monotonic()
    # processing
    result = run(
        fileset,
        processor_instance=TtbarAnalysis(USE_INFERENCE, USE_TRITON),
        treename=treename
    )
    
    exec_time = time.monotonic() - t0
    
    print(f"\nexecution took {exec_time:.2f} seconds")
    
    # utils.metrics.track_metrics(metrics, fileset, exec_time, USE_DASK, USE_SERVICEX, N_FILES_MAX_PER_SAMPLE, USE_INFERENCE, USE_TRITON)

    return result

def plotting_1(result): # <- CHANGED, do not forget to pass result

    print(result.keys())
    all_histograms, metrics = result["processor_result"]  # CHANGED
    
    utils.plotting.set_style()
    
    all_histograms["hist_dict"]["4j1b"][120j::hist.rebin(2), :, "nominal"].stack("process")[::-1].plot(stack=True, histtype="fill", linewidth=1, edgecolor="grey")
    plt.legend(frameon=False)
    plt.title(r"$\geq$ 4 jets, 1 b-tag")
    plt.xlabel("$H_T$ [GeV]");