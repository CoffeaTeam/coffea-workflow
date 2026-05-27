"""
Tests for coffea_workflow/artifacts.py
 
Covers:
  - _builder_key() stable serialisation of callable vs string builders
  - All five artifact types: keys(), type_name, identity(), to_dict(), always_rerun
  - Identity determinism: same inputs -> same hash; different inputs -> different hash
  - ARTIFACT_REGISTRY contains all registered types
"""
import pytest
from coffea_workflow.artifacts import (
    _builder_key,
    _to_params_tuple,
    Fileset,
    Chunking,
    ChunkAnalysis,
    Analysis,
    Plotting,
    CustomArtifact,
    ARTIFACT_REGISTRY,
    ArtifactBase,
)
 
 
def _some_fn():
    pass


# ---------------------------------------------------------------------------
# _to_params_tuple
# ---------------------------------------------------------------------------

class TestToParamsTuple:
    def test_none_returns_empty_tuple(self):
        assert _to_params_tuple(None) == ()

    def test_empty_dict_returns_empty_tuple(self):
        assert _to_params_tuple({}) == ()

    def test_dict_is_sorted_by_key(self):
        result = _to_params_tuple({"b": 2, "a": 1})
        assert result == (("a", 1), ("b", 2))

    def test_already_tuple_passthrough(self):
        t = (("x", 10),)
        assert _to_params_tuple(t) == t

    def test_roundtrip_dict(self):
        params = {"foo": "bar", "n": 42}
        assert dict(_to_params_tuple(params)) == params


# ---------------------------------------------------------------------------
# _builder_key
# ---------------------------------------------------------------------------
 
class TestBuilderKey:
    def test_string_passthrough(self):
        assert _builder_key("mymod:my_fn") == "mymod:my_fn"
 
    def test_callable_returns_module_colon_qualname(self):
        key = _builder_key(_some_fn)
        assert ":" in key
        assert key.endswith(":_some_fn")
 
    def test_lambda_contains_lambda_marker(self):
        fn = lambda: None
        assert "<lambda>" in _builder_key(fn)
 
    def test_same_callable_same_key(self):
        assert _builder_key(_some_fn) == _builder_key(_some_fn)
 
    def test_different_strings_differ(self):
        assert _builder_key("mod:a") != _builder_key("mod:b")
 
 
# ---------------------------------------------------------------------------
# Fileset
# ---------------------------------------------------------------------------
 
class TestFileset:
    def test_keys_contains_name_and_builder(self):
        fs = Fileset(name="my_fs", builder="mod:fn")
        k = fs.keys()
        assert k["name"] == "my_fs"
        assert k["builder"] == "mod:fn"
 
    def test_type_name(self):
        assert Fileset(name="x", builder="mod:fn").type_name == "Fileset"
 
    def test_to_dict_structure(self):
        fs = Fileset(name="x", builder="mod:fn")
        d = fs.to_dict()
        assert d["type"] == "Fileset"
        assert "keys" in d
 
    def test_identity_deterministic(self):
        assert (
            Fileset(name="x", builder="mod:fn").identity()
            == Fileset(name="x", builder="mod:fn").identity()
        )
 
    def test_identity_changes_with_name(self):
        assert (
            Fileset(name="a", builder="mod:fn").identity()
            != Fileset(name="b", builder="mod:fn").identity()
        )
 
    def test_identity_changes_with_builder(self):
        assert (
            Fileset(name="x", builder="mod:fn1").identity()
            != Fileset(name="x", builder="mod:fn2").identity()
        )
 
    def test_frozen(self):
        fs = Fileset(name="x", builder="mod:fn")
        with pytest.raises(Exception):
            fs.name = "y"

    def test_builder_params_dict_stored_as_sorted_tuple(self):
        fs = Fileset(name="x", builder="mod:fn", builder_params={"b": 2, "a": 1})
        assert fs.builder_params == (("a", 1), ("b", 2))

    def test_builder_params_keys_returns_dict(self):
        fs = Fileset(name="x", builder="mod:fn", builder_params={"k": "v"})
        assert fs.keys()["builder_params"] == {"k": "v"}

    def test_builder_params_empty_by_default(self):
        fs = Fileset(name="x", builder="mod:fn")
        assert fs.builder_params == ()
        assert fs.keys()["builder_params"] == {}

    def test_identity_changes_with_builder_params(self):
        fs1 = Fileset(name="x", builder="mod:fn", builder_params={"k": 1})
        fs2 = Fileset(name="x", builder="mod:fn", builder_params={"k": 2})
        assert fs1.identity() != fs2.identity()

    def test_identity_stable_with_same_builder_params(self):
        fs1 = Fileset(name="x", builder="mod:fn", builder_params={"k": 1})
        fs2 = Fileset(name="x", builder="mod:fn", builder_params={"k": 1})
        assert fs1.identity() == fs2.identity()

    def test_identity_differs_with_vs_without_builder_params(self):
        fs1 = Fileset(name="x", builder="mod:fn")
        fs2 = Fileset(name="x", builder="mod:fn", builder_params={"k": 1})
        assert fs1.identity() != fs2.identity()


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------
 
class TestChunking:
    @pytest.fixture
    def fs(self):
        return Fileset(name="fs", builder="mod:fn")
 
    def test_keys_fields(self, fs):
        ch = Chunking(fileset=fs, split_strategy="by_dataset", percentage=25, datasets=("A",))
        k = ch.keys()
        assert k["fileset"] is fs
        assert k["split_strategy"] == "by_dataset"
        assert k["percentage"] == 25
        assert k["datasets"] == ("A",)
 
    def test_type_name(self, fs):
        assert Chunking(fileset=fs, split_strategy=None, percentage=None).type_name == "Chunking"
 
    def test_identity_deterministic(self, fs):
        ch1 = Chunking(fileset=fs, split_strategy=None, percentage=None)
        ch2 = Chunking(fileset=fs, split_strategy=None, percentage=None)
        assert ch1.identity() == ch2.identity()
 
    def test_identity_changes_with_strategy(self, fs):
        ch1 = Chunking(fileset=fs, split_strategy=None, percentage=None)
        ch2 = Chunking(fileset=fs, split_strategy="by_dataset", percentage=None)
        assert ch1.identity() != ch2.identity()
 
    def test_identity_changes_with_percentage(self, fs):
        ch1 = Chunking(fileset=fs, split_strategy=None, percentage=None)
        ch2 = Chunking(fileset=fs, split_strategy=None, percentage=50)
        assert ch1.identity() != ch2.identity()
 
    def test_identity_changes_with_datasets(self, fs):
        ch1 = Chunking(fileset=fs, split_strategy=None, percentage=None, datasets=None)
        ch2 = Chunking(fileset=fs, split_strategy=None, percentage=None, datasets=("A",))
        assert ch1.identity() != ch2.identity()
 
 
# ---------------------------------------------------------------------------
# ChunkAnalysis
# ---------------------------------------------------------------------------
 
class TestChunkAnalysis:
    @pytest.fixture
    def chunking(self):
        fs = Fileset(name="fs", builder="mod:fn")
        return Chunking(fileset=fs, split_strategy=None, percentage=None)
 
    def test_keys_fields(self, chunking):
        ca = ChunkAnalysis(chunk_file="chunk_0.json", chunk_hash="abc123", chunking=chunking, analysis_builder="mod:run")
        k = ca.keys()
        assert k["chunk_hash"] == "abc123"
        assert k["analysis_builder"] == "mod:run"

    def test_type_name(self, chunking):
        ca = ChunkAnalysis(chunk_file="c.json", chunk_hash="abc123", chunking=chunking, analysis_builder="mod:run")
        assert ca.type_name == "ChunkAnalysis"

    def test_identity_changes_with_chunk_hash(self, chunking):
        ca1 = ChunkAnalysis(chunk_file="chunk_0.json", chunk_hash="hash_aaa", chunking=chunking, analysis_builder="mod:run")
        ca2 = ChunkAnalysis(chunk_file="chunk_0.json", chunk_hash="hash_bbb", chunking=chunking, analysis_builder="mod:run")
        assert ca1.identity() != ca2.identity()

    def test_identity_changes_with_builder(self, chunking):
        ca1 = ChunkAnalysis(chunk_file="c.json", chunk_hash="abc123", chunking=chunking, analysis_builder="mod:run_v1")
        ca2 = ChunkAnalysis(chunk_file="c.json", chunk_hash="abc123", chunking=chunking, analysis_builder="mod:run_v2")
        assert ca1.identity() != ca2.identity()

    def test_builder_params_stored_as_sorted_tuple(self, chunking):
        ca = ChunkAnalysis(chunk_file="c.json", chunk_hash="abc123", chunking=chunking, analysis_builder="mod:run", builder_params={"z": 9, "a": 1})
        assert ca.builder_params == (("a", 1), ("z", 9))

    def test_builder_params_keys_returns_dict(self, chunking):
        ca = ChunkAnalysis(chunk_file="c.json", chunk_hash="abc123", chunking=chunking, analysis_builder="mod:run", builder_params={"p": "q"})
        assert ca.keys()["builder_params"] == {"p": "q"}

    def test_builder_params_empty_by_default(self, chunking):
        ca = ChunkAnalysis(chunk_file="c.json", chunk_hash="abc123", chunking=chunking, analysis_builder="mod:run")
        assert ca.builder_params == ()
        assert ca.keys()["builder_params"] == {}

    def test_identity_changes_with_builder_params(self, chunking):
        ca1 = ChunkAnalysis(chunk_file="c.json", chunk_hash="abc123", chunking=chunking, analysis_builder="mod:run", builder_params={"k": 1})
        ca2 = ChunkAnalysis(chunk_file="c.json", chunk_hash="abc123", chunking=chunking, analysis_builder="mod:run", builder_params={"k": 2})
        assert ca1.identity() != ca2.identity()


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------
 
class TestAnalysis:
    @pytest.fixture
    def fs(self):
        return Fileset(name="fs", builder="mod:fn")
 
    def test_keys_fields(self, fs):
        an = Analysis(name="my_analysis", fileset=fs, builder="mod:run")
        k = an.keys()
        assert k["name"] == "my_analysis"
        assert k["fileset"] is fs
        assert k["builder"] == "mod:run"
 
    def test_type_name(self, fs):
        assert Analysis(name="an", fileset=fs, builder="mod:run").type_name == "Analysis"
 
    def test_always_rerun_false(self):
        assert Analysis.always_rerun is False
 
    def test_identity_deterministic(self, fs):
        an1 = Analysis(name="an", fileset=fs, builder="mod:run")
        an2 = Analysis(name="an", fileset=fs, builder="mod:run")
        assert an1.identity() == an2.identity()
 
    def test_identity_changes_with_name(self, fs):
        an1 = Analysis(name="v1", fileset=fs, builder="mod:run")
        an2 = Analysis(name="v2", fileset=fs, builder="mod:run")
        assert an1.identity() != an2.identity()
 
    def test_identity_changes_when_fileset_changes(self):
        fs1 = Fileset(name="fs1", builder="mod:fn")
        fs2 = Fileset(name="fs2", builder="mod:fn")
        an1 = Analysis(name="an", fileset=fs1, builder="mod:run")
        an2 = Analysis(name="an", fileset=fs2, builder="mod:run")
        assert an1.identity() != an2.identity()

    def test_builder_params_stored_as_sorted_tuple(self, fs):
        an = Analysis(name="an", fileset=fs, builder="mod:run", builder_params={"z": 3, "a": 1})
        assert an.builder_params == (("a", 1), ("z", 3))

    def test_builder_params_keys_returns_dict(self, fs):
        an = Analysis(name="an", fileset=fs, builder="mod:run", builder_params={"x": 42})
        assert an.keys()["builder_params"] == {"x": 42}

    def test_builder_params_empty_by_default(self, fs):
        an = Analysis(name="an", fileset=fs, builder="mod:run")
        assert an.builder_params == ()
        assert an.keys()["builder_params"] == {}

    def test_identity_changes_with_builder_params(self, fs):
        an1 = Analysis(name="an", fileset=fs, builder="mod:run", builder_params={"k": "v1"})
        an2 = Analysis(name="an", fileset=fs, builder="mod:run", builder_params={"k": "v2"})
        assert an1.identity() != an2.identity()


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------
 
class TestPlotting:
    @pytest.fixture
    def analysis(self):
        fs = Fileset(name="fs", builder="mod:fn")
        return Analysis(name="an", fileset=fs, builder="mod:run")
 
    def test_always_rerun_true(self):
        assert Plotting.always_rerun is True
 
    def test_type_name(self, analysis):
        pl = Plotting(name="p", analysis=analysis, builder="mod:plot")
        assert pl.type_name == "Plotting"
 
    def test_keys_fields(self, analysis):
        pl = Plotting(name="my_plot", analysis=analysis, builder="mod:plot")
        k = pl.keys()
        assert k["name"] == "my_plot"
        assert k["analysis"] is analysis
        assert k["builder"] == "mod:plot"
 
    def test_identity_deterministic(self, analysis):
        pl1 = Plotting(name="p", analysis=analysis, builder="mod:plot")
        pl2 = Plotting(name="p", analysis=analysis, builder="mod:plot")
        assert pl1.identity() == pl2.identity()
 
    def test_identity_changes_with_name(self, analysis):
        pl1 = Plotting(name="p1", analysis=analysis, builder="mod:plot")
        pl2 = Plotting(name="p2", analysis=analysis, builder="mod:plot")
        assert pl1.identity() != pl2.identity()

    def test_builder_params_stored_as_sorted_tuple(self, analysis):
        pl = Plotting(name="p", analysis=analysis, builder="mod:plot", builder_params={"y": 0, "a": 5})
        assert pl.builder_params == (("a", 5), ("y", 0))

    def test_builder_params_keys_returns_dict(self, analysis):
        pl = Plotting(name="p", analysis=analysis, builder="mod:plot", builder_params={"color": "red"})
        assert pl.keys()["builder_params"] == {"color": "red"}

    def test_builder_params_empty_by_default(self, analysis):
        pl = Plotting(name="p", analysis=analysis, builder="mod:plot")
        assert pl.builder_params == ()
        assert pl.keys()["builder_params"] == {}

    def test_identity_changes_with_builder_params(self, analysis):
        pl1 = Plotting(name="p", analysis=analysis, builder="mod:plot", builder_params={"bins": 10})
        pl2 = Plotting(name="p", analysis=analysis, builder="mod:plot", builder_params={"bins": 20})
        assert pl1.identity() != pl2.identity()


# ---------------------------------------------------------------------------
# ARTIFACT_REGISTRY
# ---------------------------------------------------------------------------
 
class TestArtifactRegistry:
    @pytest.mark.parametrize("name", ["Fileset", "Chunking", "ChunkAnalysis", "Analysis", "Plotting"])
    def test_type_is_registered(self, name):
        assert name in ARTIFACT_REGISTRY

    def test_registry_values_are_artifact_base_subclasses(self):
        for cls in ARTIFACT_REGISTRY.values():
            assert issubclass(cls, ArtifactBase)


# ---------------------------------------------------------------------------
# Flexible upstream: CustomArtifact as fileset / analysis
# ---------------------------------------------------------------------------

@pytest.fixture
def custom_fileset_art():
    fs = Fileset(name="fs", builder="mod:fn")
    return CustomArtifact(name="my_filter", builder="mod:filter", upstreams=(fs,))


class TestChunkingAcceptsCustomArtifact:
    def test_custom_artifact_accepted_as_fileset(self, custom_fileset_art):
        ch = Chunking(fileset=custom_fileset_art, split_strategy=None, percentage=None)
        assert ch.fileset is custom_fileset_art

    def test_identity_differs_from_plain_fileset(self, custom_fileset_art):
        fs = Fileset(name="fs", builder="mod:fn")
        ch_plain = Chunking(fileset=fs, split_strategy=None, percentage=None)
        ch_custom = Chunking(fileset=custom_fileset_art, split_strategy=None, percentage=None)
        assert ch_plain.identity() != ch_custom.identity()

    def test_identity_deterministic_with_custom_upstream(self, custom_fileset_art):
        ch1 = Chunking(fileset=custom_fileset_art, split_strategy=None, percentage=None)
        ch2 = Chunking(fileset=custom_fileset_art, split_strategy=None, percentage=None)
        assert ch1.identity() == ch2.identity()


class TestAnalysisAcceptsCustomArtifact:
    def test_custom_artifact_accepted_as_fileset(self, custom_fileset_art):
        an = Analysis(name="an", fileset=custom_fileset_art, builder="mod:run")
        assert an.fileset is custom_fileset_art

    def test_identity_differs_from_plain_fileset(self, custom_fileset_art):
        fs = Fileset(name="fs", builder="mod:fn")
        an_plain = Analysis(name="an", fileset=fs, builder="mod:run")
        an_custom = Analysis(name="an", fileset=custom_fileset_art, builder="mod:run")
        assert an_plain.identity() != an_custom.identity()

    def test_identity_deterministic_with_custom_upstream(self, custom_fileset_art):
        an1 = Analysis(name="an", fileset=custom_fileset_art, builder="mod:run")
        an2 = Analysis(name="an", fileset=custom_fileset_art, builder="mod:run")
        assert an1.identity() == an2.identity()


class TestPlottingAcceptsCustomArtifact:
    def test_custom_artifact_accepted_as_analysis(self, custom_fileset_art):
        fs = Fileset(name="fs", builder="mod:fn")
        analysis = Analysis(name="an", fileset=fs, builder="mod:run")
        post_custom = CustomArtifact(name="post", builder="mod:post", upstreams=(analysis,))
        pl = Plotting(name="p", analysis=post_custom, builder="mod:plot")
        assert pl.analysis is post_custom

    def test_identity_differs_from_plain_analysis(self):
        fs = Fileset(name="fs", builder="mod:fn")
        analysis = Analysis(name="an", fileset=fs, builder="mod:run")
        post_custom = CustomArtifact(name="post", builder="mod:post", upstreams=(analysis,))
        pl_plain = Plotting(name="p", analysis=analysis, builder="mod:plot")
        pl_custom = Plotting(name="p", analysis=post_custom, builder="mod:plot")
        assert pl_plain.identity() != pl_custom.identity()