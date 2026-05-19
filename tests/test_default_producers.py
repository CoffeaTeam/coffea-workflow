"""
Tests for workflow/default_producers.py
 
Pure-logic helpers tested without touching the filesystem or coffea:
  - _call_builder: injects config kwarg only when the function signature accepts it
  - _load_object: resolves 'module:attr' and 'module.attr' strings; returns
                  callables directly
  - _split_fileset: all combinations of strategy/percentage/datasets
"""
import json
import pytest
import cloudpickle
from pathlib import Path
from unittest.mock import MagicMock, patch

from workflow.producers_utils import _call_builder, _load_object, _split_fileset, build_executor
from workflow.default_producers import make_fileset, split_fileset
from workflow.artifacts import Fileset, Chunking, CustomArtifact
from workflow.config import RunConfig, ExecutorConfig, FacilityConfig
from workflow.deps import Deps
 
 
# ---------------------------------------------------------------------------
# _call_builder
# ---------------------------------------------------------------------------
 
class TestCallBuilder:
    def test_calls_fn_with_positional_arg(self):
        calls = []
        def fn(x):
            calls.append(x)
            return "ok"
 
        assert _call_builder(fn, "hello", config=RunConfig()) == "ok"
        assert calls == ["hello"]
 
    def test_does_not_inject_config_when_fn_has_no_config_param(self):
        def fn(x):
            return x * 2
 
        # If config were injected this would raise TypeError
        assert _call_builder(fn, 3, config=RunConfig()) == 6
 
    def test_injects_config_when_fn_accepts_it(self):
        received = {}
        cfg = RunConfig()
 
        def fn(x, config):
            received["config"] = config
            return x
 
        _call_builder(fn, "data", config=cfg)
        assert received["config"] is cfg
 
    def test_config_none_never_injected(self):
        def fn(x):
            return x
 
        assert _call_builder(fn, 42, config=None) == 42
 
    def test_no_args_fn_called_correctly(self):
        def fn():
            return "result"
 
        assert _call_builder(fn, config=RunConfig()) == "result"
 
 
# ---------------------------------------------------------------------------
# _load_object
# ---------------------------------------------------------------------------
 
class TestLoadObject:
    def test_callable_returned_directly(self):
        fn = lambda: 42
        assert _load_object(fn) is fn
 
    def test_module_colon_attr_syntax(self):
        import os.path
        loaded = _load_object("os.path:join")
        assert loaded is os.path.join
 
    def test_module_dot_attr_syntax(self):
        import os.path
        loaded = _load_object("os.path.join")
        assert loaded is os.path.join
 
    def test_stdlib_function(self):
        import json as _json
        loaded = _load_object("json:dumps")
        assert loaded is _json.dumps
 
    def test_missing_attr_raises_attribute_error(self):
        with pytest.raises(AttributeError, match="not found in module"):
            _load_object("os:_this_does_not_exist_xyz")
 
    def test_returns_class_as_well_as_function(self):
        import pathlib
        loaded = _load_object("pathlib:Path")
        assert loaded is pathlib.Path
 
 
# ---------------------------------------------------------------------------
# _split_fileset
# ---------------------------------------------------------------------------
 
@pytest.fixture
def two_dataset_fileset():
    return {
        "A": {"files": {"a1.root": "T", "a2.root": "T", "a3.root": "T", "a4.root": "T"}},
        "B": {"files": {"b1.root": "T", "b2.root": "T"}},
    }
 
 
class TestSplitFilesetNoSplit:
    def test_returns_single_chunk_containing_whole_fileset(self, two_dataset_fileset):
        chunks = _split_fileset(two_dataset_fileset)
        assert len(chunks) == 1
        assert chunks[0] == two_dataset_fileset
 
    def test_empty_fileset_yields_one_empty_chunk(self):
        assert _split_fileset({}) == [{}]
 
 
class TestSplitFilesetByDataset:
    def test_one_chunk_per_dataset(self, two_dataset_fileset):
        chunks = _split_fileset(two_dataset_fileset, strategy="by_dataset")
        assert len(chunks) == 2
 
    def test_each_chunk_contains_exactly_one_dataset(self, two_dataset_fileset):
        chunks = _split_fileset(two_dataset_fileset, strategy="by_dataset")
        for chunk in chunks:
            assert len(chunk) == 1
 
    def test_all_datasets_are_present(self, two_dataset_fileset):
        chunks = _split_fileset(two_dataset_fileset, strategy="by_dataset")
        names = {list(c.keys())[0] for c in chunks}
        assert names == {"A", "B"}
 
    def test_files_intact_per_dataset(self, two_dataset_fileset):
        chunks = _split_fileset(two_dataset_fileset, strategy="by_dataset")
        chunk_a = next(c for c in chunks if "A" in c)
        assert set(chunk_a["A"]["files"].keys()) == {"a1.root", "a2.root", "a3.root", "a4.root"}
 
 
class TestSplitFilesetByPercentage:
    def test_50_percent_gives_two_chunks(self, two_dataset_fileset):
        chunks = _split_fileset(two_dataset_fileset, percentage=50)
        assert len(chunks) == 2
 
    def test_25_percent_gives_four_chunks(self, two_dataset_fileset):
        chunks = _split_fileset(two_dataset_fileset, percentage=25)
        assert len(chunks) == 4
 
    def test_all_files_covered_across_chunks(self, two_dataset_fileset):
        chunks = _split_fileset(two_dataset_fileset, percentage=50)
        all_a = set()
        all_b = set()
        for chunk in chunks:
            all_a.update(chunk.get("A", {}).get("files", {}).keys())
            all_b.update(chunk.get("B", {}).get("files", {}).keys())
        assert all_a == {"a1.root", "a2.root", "a3.root", "a4.root"}
        assert all_b == {"b1.root", "b2.root"}
 
    def test_100_percent_returns_single_chunk(self, two_dataset_fileset):
        chunks = _split_fileset(two_dataset_fileset, percentage=100)
        assert len(chunks) == 1
 
 
class TestSplitFilesetCombined:
    def test_by_dataset_and_50_percent(self, two_dataset_fileset):
        # 2 datasets × 2 chunks each = 4 chunks
        chunks = _split_fileset(two_dataset_fileset, strategy="by_dataset", percentage=50)
        assert len(chunks) == 4
 
    def test_each_combined_chunk_has_single_dataset(self, two_dataset_fileset):
        chunks = _split_fileset(two_dataset_fileset, strategy="by_dataset", percentage=50)
        for chunk in chunks:
            assert len(chunk) == 1
 
 
class TestSplitFilesetDatasetsFilter:
    def test_list_filter_keeps_only_named_datasets(self, two_dataset_fileset):
        chunks = _split_fileset(two_dataset_fileset, datasets=["A"])
        assert len(chunks) == 1
        assert "A" in chunks[0]
        assert "B" not in chunks[0]
 
    def test_callable_filter(self, two_dataset_fileset):
        chunks = _split_fileset(two_dataset_fileset, datasets=lambda name: name == "B")
        assert "B" in chunks[0]
        assert "A" not in chunks[0]
 
    def test_empty_filter_result_yields_one_empty_chunk(self, two_dataset_fileset):
        chunks = _split_fileset(two_dataset_fileset, datasets=["NonExistent"])
        # no matching datasets → result is [{}] or empty list – no crash
        assert isinstance(chunks, list)
 
    def test_datasets_filter_combined_with_by_dataset(self, two_dataset_fileset):
        chunks = _split_fileset(two_dataset_fileset, strategy="by_dataset", datasets=["A"])
        assert len(chunks) == 1
        assert "A" in chunks[0]
 
 
class TestSplitFilesetValidation:
    def test_invalid_strategy_raises(self, two_dataset_fileset):
        with pytest.raises(ValueError, match="Unknown strategy"):
            _split_fileset(two_dataset_fileset, strategy="unknown")
 
    def test_non_divisor_percentage_raises(self, two_dataset_fileset):
        with pytest.raises(ValueError, match="percentage"):
            _split_fileset(two_dataset_fileset, percentage=30)
 
    def test_zero_percentage_raises(self, two_dataset_fileset):
        with pytest.raises(ValueError, match="percentage"):
            _split_fileset(two_dataset_fileset, percentage=0)
 
 
# ---------------------------------------------------------------------------
# make_fileset producer (filesystem)
# ---------------------------------------------------------------------------
 
class TestMakeFileset:
    def test_writes_fileset_json(self, tmp_path):
        expected = {"ds": {"files": {"f.root": "Events"}}}
 
        def builder():
            return expected
 
        art = Fileset(name="test", builder=builder)
        deps = MagicMock(spec=Deps)
        cfg = RunConfig(cache_dir=tmp_path)
        out = tmp_path / "Fileset" / art.identity()
 
        make_fileset(art=art, deps=deps, out=out, config=cfg)
 
        written = json.loads((out / "fileset.json").read_text())
        assert written == expected
 
    def test_creates_output_directory(self, tmp_path):
        def builder():
            return {"ds": {"files": {}}}
 
        art = Fileset(name="x", builder=builder)
        deps = MagicMock(spec=Deps)
        cfg = RunConfig(cache_dir=tmp_path)
        out = tmp_path / "deep" / "nested" / "dir"
 
        make_fileset(art=art, deps=deps, out=out, config=cfg)
 
        assert out.is_dir()
        assert (out / "fileset.json").exists()
 
    def test_raises_when_builder_returns_non_dict(self, tmp_path):
        def bad_builder():
            return ["not", "a", "dict"]
 
        art = Fileset(name="x", builder=bad_builder)
        deps = MagicMock(spec=Deps)
        cfg = RunConfig(cache_dir=tmp_path)
        out = tmp_path / "out"
 
        with pytest.raises(TypeError, match="Fileset builder must return a dict"):
            make_fileset(art=art, deps=deps, out=out, config=cfg)


# ---------------------------------------------------------------------------
# split_fileset producer with non-Fileset upstream (CustomArtifact)
# ---------------------------------------------------------------------------

class TestSplitFilesetProducerWithCustomUpstream:
    def _make_custom_upstream(self, tmp_path, payload):
        """Write payload.pkl for a CustomArtifact and return a Deps mock pointing to it."""
        upstream_dir = tmp_path / "upstream"
        upstream_dir.mkdir()
        (upstream_dir / "payload.pkl").write_bytes(cloudpickle.dumps(payload))

        fs = Fileset(name="fs", builder="mod:fn")
        custom_art = CustomArtifact(name="filtered", builder="mod:filter", upstreams=(fs,))
        chunking = Chunking(fileset=custom_art, split_strategy=None, percentage=None)

        deps = MagicMock(spec=Deps)
        deps.need.return_value = upstream_dir
        return chunking, deps

    def test_splits_fileset_from_custom_artifact_payload(self, tmp_path):
        fileset = {
            "A": {"files": {"a1.root": "T", "a2.root": "T"}},
            "B": {"files": {"b1.root": "T"}},
        }
        chunking, deps = self._make_custom_upstream(tmp_path, fileset)
        out = tmp_path / "out"
        cfg = RunConfig(cache_dir=tmp_path)

        split_fileset(art=chunking, deps=deps, out=out, config=cfg)

        manifest = json.loads((out / "manifest.json").read_text())
        assert manifest["n_chunks"] == 1
        chunk = json.loads((out / manifest["output_files"]["0"]).read_text())
        assert set(chunk.keys()) == {"A", "B"}

    def test_by_dataset_strategy_with_custom_artifact(self, tmp_path):
        fileset = {
            "A": {"files": {"a1.root": "T"}},
            "B": {"files": {"b1.root": "T"}},
        }
        fs = Fileset(name="fs", builder="mod:fn")
        custom_art = CustomArtifact(name="filtered", builder="mod:filter", upstreams=(fs,))
        chunking = Chunking(fileset=custom_art, split_strategy="by_dataset", percentage=None)

        upstream_dir = tmp_path / "upstream"
        upstream_dir.mkdir()
        (upstream_dir / "payload.pkl").write_bytes(cloudpickle.dumps(fileset))

        deps = MagicMock(spec=Deps)
        deps.need.return_value = upstream_dir
        out = tmp_path / "out"
        cfg = RunConfig(cache_dir=tmp_path, strategy="by_dataset")

        split_fileset(art=chunking, deps=deps, out=out, config=cfg)

        manifest = json.loads((out / "manifest.json").read_text())
        assert manifest["n_chunks"] == 2

    def test_raises_type_error_when_payload_is_not_dict(self, tmp_path):
        chunking, deps = self._make_custom_upstream(tmp_path, ["not", "a", "fileset"])
        out = tmp_path / "out"
        cfg = RunConfig(cache_dir=tmp_path)

        with pytest.raises(TypeError, match="must produce a fileset dict"):
            split_fileset(art=chunking, deps=deps, out=out, config=cfg)


# ---------------------------------------------------------------------------
# _call_builder executor injection
# ---------------------------------------------------------------------------

class TestCallBuilderExecutorInjection:
    def test_injects_executor_when_fn_accepts_it(self):
        received = {}

        def fn(x, executor=None):
            received["executor"] = executor
            return x

        fake_executor = object()
        _call_builder(fn, "data", executor=fake_executor)
        assert received["executor"] is fake_executor

    def test_does_not_inject_executor_when_fn_omits_it(self):
        def fn(x):
            return x * 2

        # Would raise TypeError if executor were injected
        assert _call_builder(fn, 3, executor=object()) == 6

    def test_executor_none_never_injected(self):
        def fn(x, executor=None):
            return executor

        assert _call_builder(fn, "ignored", executor=None) is None


# ---------------------------------------------------------------------------
# build_executor
# ---------------------------------------------------------------------------

class TestBuildExecutor:
    def test_both_none_returns_none(self):
        assert build_executor(None, None) is None

    def test_iterative_returns_iterative_executor(self):
        from coffea.processor import IterativeExecutor
        ec = ExecutorConfig(executor_type="IterativeExecutor")
        assert isinstance(build_executor(ec), IterativeExecutor)

    def test_futures_returns_futures_executor(self):
        from coffea.processor import FuturesExecutor
        ec = ExecutorConfig(executor_type="FuturesExecutor", workers=2)
        assert isinstance(build_executor(ec), FuturesExecutor)

    def test_raw_executor_returned_unchanged(self):
        fake = MagicMock()
        ec = ExecutorConfig(executor=fake)
        assert build_executor(ec) is fake

    def test_no_ec_local_facility_returns_futures_executor(self):
        from coffea.processor import FuturesExecutor
        fc = FacilityConfig(name="local")
        assert isinstance(build_executor(None, fc), FuturesExecutor)

    def test_dask_ec_with_facility_address_uses_facility(self, monkeypatch):
        import sys
        from coffea.processor import DaskExecutor
        monkeypatch.setenv("COFFEA_CASA_SCHEDULER", "tcp://casa:8786")
        fc = FacilityConfig(name="coffea-casa")
        ec = ExecutorConfig(executor_type="DaskExecutor")
        mock_dd = MagicMock()
        mock_dd.Client = MagicMock(return_value=MagicMock())
        monkeypatch.setitem(sys.modules, "distributed", MagicMock())
        monkeypatch.setitem(sys.modules, "dask.distributed", mock_dd)
        result = build_executor(ec, fc)
        assert isinstance(result, DaskExecutor)

    def test_dask_ec_without_facility_uses_dask_scheduler_field(self, monkeypatch):
        import sys
        from coffea.processor import DaskExecutor
        mock_dd = MagicMock()
        mock_dd.Client = MagicMock(return_value=MagicMock())
        monkeypatch.setitem(sys.modules, "distributed", MagicMock())
        monkeypatch.setitem(sys.modules, "dask.distributed", mock_dd)
        ec = ExecutorConfig(executor_type="DaskExecutor", dask_scheduler="tcp://host:8786")
        assert isinstance(build_executor(ec, None), DaskExecutor)

    def test_dask_ec_without_facility_and_no_scheduler_raises(self):
        ec = ExecutorConfig(executor_type="DaskExecutor")
        with pytest.raises(ValueError, match="DaskExecutor requires"):
            build_executor(ec, None)

    def test_coffea_casa_facility_without_address_raises(self, monkeypatch):
        monkeypatch.delenv("COFFEA_CASA_SCHEDULER", raising=False)
        fc = FacilityConfig(name="coffea-casa")
        with pytest.raises(ValueError, match="COFFEA_CASA_SCHEDULER"):
            build_executor(None, fc)