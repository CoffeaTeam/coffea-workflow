"""
Tests for workflow/executor.py
 
Executor.path_for() builds the content-addressed cache path.
Executor.exists() checks whether the expected sentinel file is present and
handles Analysis-specific flags (.has_failures, .chunk_fraction).
Executor.materialize() short-circuits on the session cache, falls back to
the disk cache, calls the producer otherwise, and raises if the producer
creates no output.
"""
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
 
from workflow.executor import Executor
from workflow.config import RunConfig
from workflow.artifacts import Fileset, Chunking, ChunkAnalysis, Analysis, Plotting
 
 
# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
 
def _make_executor(tmp_path, **config_kwargs):
    cfg = RunConfig(cache_dir=tmp_path, **config_kwargs)
    return Executor(tmp_path, cfg)
 
 
def _touch_sentinel(executor, art, sentinel_name):
    """Create the expected sentinel file so exists() returns True."""
    p = executor.path_for(art)
    p.mkdir(parents=True, exist_ok=True)
    (p / sentinel_name).write_text("")
    return p
 
 
# ---------------------------------------------------------------------------
# path_for
# ---------------------------------------------------------------------------
 
class TestPathFor:
    def test_path_is_cache_slash_type_slash_identity(self, tmp_path):
        ex = _make_executor(tmp_path)
        fs = Fileset(name="x", builder="mod:fn")
        assert ex.path_for(fs) == tmp_path / "Fileset" / fs.identity()
 
    def test_path_for_analysis(self, tmp_path):
        ex = _make_executor(tmp_path)
        fs = Fileset(name="x", builder="mod:fn")
        an = Analysis(name="an", fileset=fs, builder="mod:run")
        assert ex.path_for(an) == tmp_path / "Analysis" / an.identity()
 
    def test_different_artifacts_different_paths(self, tmp_path):
        ex = _make_executor(tmp_path)
        fs1 = Fileset(name="a", builder="mod:fn")
        fs2 = Fileset(name="b", builder="mod:fn")
        assert ex.path_for(fs1) != ex.path_for(fs2)
 
 
# ---------------------------------------------------------------------------
# exists
# ---------------------------------------------------------------------------
 
class TestExistsFileset:
    def test_false_when_directory_missing(self, tmp_path):
        ex = _make_executor(tmp_path)
        assert ex.exists(Fileset(name="x", builder="mod:fn")) is False
 
    def test_false_when_dir_exists_but_fileset_json_missing(self, tmp_path):
        ex = _make_executor(tmp_path)
        fs = Fileset(name="x", builder="mod:fn")
        ex.path_for(fs).mkdir(parents=True)
        assert ex.exists(fs) is False
 
    def test_true_when_fileset_json_present(self, tmp_path):
        ex = _make_executor(tmp_path)
        fs = Fileset(name="x", builder="mod:fn")
        _touch_sentinel(ex, fs, "fileset.json")
        assert ex.exists(fs) is True
 
 
class TestExistsChunking:
    def test_false_when_manifest_missing(self, tmp_path):
        ex = _make_executor(tmp_path)
        fs = Fileset(name="x", builder="mod:fn")
        ch = Chunking(fileset=fs, split_strategy=None, percentage=None)
        ex.path_for(ch).mkdir(parents=True)
        assert ex.exists(ch) is False
 
    def test_true_when_manifest_present(self, tmp_path):
        ex = _make_executor(tmp_path)
        fs = Fileset(name="x", builder="mod:fn")
        ch = Chunking(fileset=fs, split_strategy=None, percentage=None)
        _touch_sentinel(ex, ch, "manifest.json")
        assert ex.exists(ch) is True
 
 
class TestExistsChunkAnalysis:
    def test_false_when_success_missing(self, tmp_path):
        ex = _make_executor(tmp_path)
        fs = Fileset(name="x", builder="mod:fn")
        ch = Chunking(fileset=fs, split_strategy=None, percentage=None)
        ca = ChunkAnalysis(chunk_file="c.json", chunking=ch, analysis_builder="mod:run")
        ex.path_for(ca).mkdir(parents=True)
        assert ex.exists(ca) is False
 
    def test_true_when_success_present(self, tmp_path):
        ex = _make_executor(tmp_path)
        fs = Fileset(name="x", builder="mod:fn")
        ch = Chunking(fileset=fs, split_strategy=None, percentage=None)
        ca = ChunkAnalysis(chunk_file="c.json", chunking=ch, analysis_builder="mod:run")
        _touch_sentinel(ex, ca, ".success")
        assert ex.exists(ca) is True
 
 
class TestExistsAnalysis:
    @pytest.fixture
    def analysis(self):
        fs = Fileset(name="fs", builder="mod:fn")
        return Analysis(name="an", fileset=fs, builder="mod:run")
 
    def _setup_complete(self, ex, an, chunk_fraction="None"):
        p = ex.path_for(an)
        p.mkdir(parents=True, exist_ok=True)
        (p / "payload.pkl").write_bytes(b"data")
        (p / ".chunk_fraction").write_text(chunk_fraction)
        return p
 
    def test_true_when_complete(self, tmp_path, analysis):
        ex = _make_executor(tmp_path)
        self._setup_complete(ex, analysis)
        assert ex.exists(analysis) is True
 
    def test_false_when_payload_missing(self, tmp_path, analysis):
        ex = _make_executor(tmp_path)
        p = ex.path_for(analysis)
        p.mkdir(parents=True)
        (p / ".chunk_fraction").write_text("None")
        assert ex.exists(analysis) is False
 
    def test_false_when_has_failures_present(self, tmp_path, analysis):
        ex = _make_executor(tmp_path)
        p = self._setup_complete(ex, analysis)
        (p / ".has_failures").touch()
        assert ex.exists(analysis) is False
 
    def test_false_when_chunk_fraction_mismatch(self, tmp_path, analysis):
        ex = _make_executor(tmp_path, chunk_fraction=0.5)
        p = self._setup_complete(ex, analysis, chunk_fraction="None")
        assert ex.exists(analysis) is False
 
    def test_true_when_chunk_fraction_matches(self, tmp_path, analysis):
        ex = _make_executor(tmp_path, chunk_fraction=0.5)
        self._setup_complete(ex, analysis, chunk_fraction="0.5")
        assert ex.exists(analysis) is True
 
 
# ---------------------------------------------------------------------------
# materialize
# ---------------------------------------------------------------------------
 
class TestMaterialize:
    def test_returns_path_immediately_from_session_cache(self, tmp_path):
        ex = _make_executor(tmp_path)
        fs = Fileset(name="x", builder="mod:fn")
        p = ex.path_for(fs)
        p.mkdir(parents=True, exist_ok=True)
        ex._session_cache.add(p)
 
        with patch("workflow.executor.get_producer") as mock_get:
            result = ex.materialize(fs)
 
        mock_get.assert_not_called()
        assert result == p
 
    def test_returns_path_from_disk_cache_without_calling_producer(self, tmp_path):
        ex = _make_executor(tmp_path)
        fs = Fileset(name="x", builder="mod:fn")
        _touch_sentinel(ex, fs, "fileset.json")
 
        with patch("workflow.executor.get_producer") as mock_get:
            result = ex.materialize(fs)
 
        mock_get.assert_not_called()
        assert result == ex.path_for(fs)
 
    def test_disk_cache_hit_adds_to_session_cache(self, tmp_path):
        ex = _make_executor(tmp_path)
        fs = Fileset(name="x", builder="mod:fn")
        _touch_sentinel(ex, fs, "fileset.json")
        ex.materialize(fs)
        assert ex.path_for(fs) in ex._session_cache
 
    def test_calls_producer_when_not_cached(self, tmp_path):
        ex = _make_executor(tmp_path)
        fs = Fileset(name="x", builder="mod:fn")
 
        def fake_producer(*, art, deps, out, config):
            out.mkdir(parents=True, exist_ok=True)
            (out / "fileset.json").write_text("{}")
 
        with patch("workflow.executor.get_producer", return_value=fake_producer):
            result = ex.materialize(fs)
 
        assert result == ex.path_for(fs)
        assert (result / "fileset.json").exists()
 
    def test_producer_result_added_to_session_cache(self, tmp_path):
        ex = _make_executor(tmp_path)
        fs = Fileset(name="x", builder="mod:fn")
 
        def fake_producer(*, art, deps, out, config):
            out.mkdir(parents=True, exist_ok=True)
            (out / "fileset.json").write_text("{}")
 
        with patch("workflow.executor.get_producer", return_value=fake_producer):
            path = ex.materialize(fs)
 
        assert path in ex._session_cache
 
    def test_raises_runtime_error_when_producer_creates_no_output(self, tmp_path):
        ex = _make_executor(tmp_path)
        fs = Fileset(name="x", builder="mod:fn")
 
        def bad_producer(*, art, deps, out, config):
            pass  # intentionally creates nothing
 
        with patch("workflow.executor.get_producer", return_value=bad_producer):
            with pytest.raises(RuntimeError, match="did not create output"):
                ex.materialize(fs)
 
    def test_always_rerun_artifact_skips_disk_cache(self, tmp_path):
        ex = _make_executor(tmp_path)
        fs = Fileset(name="fs", builder="mod:fn")
        an = Analysis(name="an", fileset=fs, builder="mod:run")
        pl = Plotting(name="p", analysis=an, builder="mod:plot")
 
        # Pre-populate disk cache with payload.pkl
        p = ex.path_for(pl)
        p.mkdir(parents=True, exist_ok=True)
        (p / "payload.pkl").write_bytes(b"old")
 
        producer_called = []
 
        def fake_producer(*, art, deps, out, config):
            producer_called.append(True)
            (out / "payload.pkl").write_bytes(b"new")
 
        with patch("workflow.executor.get_producer", return_value=fake_producer):
            ex.materialize(pl)
 
        assert producer_called, "Producer should be called for always_rerun artifacts"


# ---------------------------------------------------------------------------
# per-step config param on exists() and materialize()
# ---------------------------------------------------------------------------

class TestExistsWithConfig:
    @pytest.fixture
    def analysis(self):
        fs = Fileset(name="fs", builder="mod:fn")
        return Analysis(name="an", fileset=fs, builder="mod:run")

    def _setup_complete(self, ex, an, chunk_fraction="None"):
        p = ex.path_for(an)
        p.mkdir(parents=True, exist_ok=True)
        (p / "payload.pkl").write_bytes(b"data")
        (p / ".chunk_fraction").write_text(chunk_fraction)
        return p

    def test_explicit_config_used_for_chunk_fraction_check(self, tmp_path, analysis):
        # executor has chunk_fraction=None; we pass a config with chunk_fraction=0.5
        # cache was written with 0.5 -> should be a hit
        ex = _make_executor(tmp_path)
        self._setup_complete(ex, analysis, chunk_fraction="0.5")
        per_step = RunConfig(cache_dir=tmp_path, chunk_fraction=0.5)
        assert ex.exists(analysis, config=per_step) is True

    def test_explicit_config_mismatch_is_a_miss(self, tmp_path, analysis):
        # cache was written with 0.5 but we query with 0.25 -> miss
        ex = _make_executor(tmp_path)
        self._setup_complete(ex, analysis, chunk_fraction="0.5")
        per_step = RunConfig(cache_dir=tmp_path, chunk_fraction=0.25)
        assert ex.exists(analysis, config=per_step) is False

    def test_falls_back_to_self_config_when_none(self, tmp_path, analysis):
        ex = _make_executor(tmp_path)
        self._setup_complete(ex, analysis)
        # no explicit config -> uses self.config (chunk_fraction=None, stamp="None") -> hit
        assert ex.exists(analysis) is True


class TestMaterializeWithConfig:
    def test_explicit_config_passed_to_producer(self, tmp_path):
        ex = _make_executor(tmp_path)
        fs = Fileset(name="x", builder="mod:fn")
        per_step = RunConfig(cache_dir=tmp_path, chunk_fraction=0.5)
        received = {}

        def fake_producer(*, art, deps, out, config):
            received["config"] = config
            out.mkdir(parents=True, exist_ok=True)
            (out / "fileset.json").write_text("{}")

        with patch("workflow.executor.get_producer", return_value=fake_producer):
            ex.materialize(fs, config=per_step)

        assert received["config"].chunk_fraction == 0.5

    def test_self_config_used_when_no_explicit_config(self, tmp_path):
        ex = _make_executor(tmp_path, chunk_fraction=0.3)
        fs = Fileset(name="x", builder="mod:fn")
        received = {}

        def fake_producer(*, art, deps, out, config):
            received["config"] = config
            out.mkdir(parents=True, exist_ok=True)
            (out / "fileset.json").write_text("{}")

        with patch("workflow.executor.get_producer", return_value=fake_producer):
            ex.materialize(fs)

        assert received["config"].chunk_fraction == 0.3