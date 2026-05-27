import pytest
from pathlib import Path
from coffea_workflow.artifacts import Fileset, Analysis, Chunking, Plotting, ChunkAnalysis
from coffea_workflow.config import RunConfig
 
 
@pytest.fixture
def basic_fileset():
    return Fileset(name="my_fileset", builder="mymodule:get_fileset")
 
 
@pytest.fixture
def basic_config(tmp_path):
    return RunConfig(cache_dir=tmp_path)
 
 
@pytest.fixture
def sample_fileset_dict():
    return {
        "datasetA": {
            "files": {
                "file1.root": "Events",
                "file2.root": "Events",
                "file3.root": "Events",
                "file4.root": "Events",
            }
        },
        "datasetB": {
            "files": {
                "file5.root": "Events",
                "file6.root": "Events",
            }
        },
    }