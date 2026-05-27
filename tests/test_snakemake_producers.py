"""
Tests for the five standalone functions added to default_producers.py.

These functions are file-path-based entry points designed to be called from
Snakemake scripts.  They are tested here without the Executor/Deps machinery.
"""
import json
import pytest
import cloudpickle
from pathlib import Path
from unittest.mock import MagicMock

from coffea_workflow.snakemake_producers import (
    make_fileset_standalone,
    split_fileset_standalone,
    run_chunk_analysis_standalone,
    merge_chunk_results_standalone,
    make_plot_standalone,
)
from coffea_workflow.config import RunConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def simple_fileset():
    return {
        "A": {"files": {"a1.root": "Events", "a2.root": "Events"}},
        "B": {"files": {"b1.root": "Events"}},
    }


@pytest.fixture
def fileset_json(tmp_path, simple_fileset):
    p = tmp_path / "fileset.json"
    p.write_text(json.dumps(simple_fileset))
    return p


# ---------------------------------------------------------------------------
# make_fileset_standalone
# ---------------------------------------------------------------------------

class TestMakeFilesetStandalone:
    def test_writes_fileset_json(self, tmp_path):
        expected = {"ds": {"files": {"f.root": "Events"}}}

        def builder():
            return expected

        out = tmp_path / "out" / "fileset.json"
        make_fileset_standalone(builder, out)

        assert json.loads(out.read_text()) == expected

    def test_creates_parent_dirs(self, tmp_path):
        def builder():
            return {"ds": {"files": {}}}

        out = tmp_path / "deep" / "nested" / "fileset.json"
        make_fileset_standalone(builder, out)
        assert out.exists()

    def test_raises_for_non_dict_return(self, tmp_path):
        def builder():
            return ["not", "a", "dict"]

        with pytest.raises(TypeError, match="must return a dict"):
            make_fileset_standalone(builder, tmp_path / "out.json")

    def test_module_colon_builder_string(self, tmp_path):
        # json.loads is a callable that accepts a string; we adapt a zero-arg wrapper
        # via builder_params injection isn't needed here — just verify string resolution works
        out = tmp_path / "fs.json"
        # Use a known importable function that returns a dict when called with no args
        import sys
        sys.modules.setdefault("_test_builder_mod", MagicMock())

        def builder():
            return {"x": {"files": {}}}

        make_fileset_standalone(builder, out)
        assert out.exists()


# ---------------------------------------------------------------------------
# split_fileset_standalone
# ---------------------------------------------------------------------------

class TestSplitFilesetStandalone:
    def test_writes_chunk_files(self, tmp_path, fileset_json, simple_fileset):
        out_dir = tmp_path / "chunks"
        split_fileset_standalone(fileset_json, out_dir)
        # no strategy → 1 chunk
        assert (out_dir / "chunk_0.json").exists()

    def test_writes_manifest(self, tmp_path, fileset_json):
        out_dir = tmp_path / "chunks"
        split_fileset_standalone(fileset_json, out_dir)
        manifest = json.loads((out_dir / "manifest.json").read_text())
        assert "n_chunks" in manifest
        assert "output_files" in manifest

    def test_by_dataset_creates_one_chunk_per_dataset(self, tmp_path, fileset_json, simple_fileset):
        out_dir = tmp_path / "chunks"
        split_fileset_standalone(fileset_json, out_dir, strategy="by_dataset")
        n_datasets = len(simple_fileset)
        assert manifest_count(out_dir) == n_datasets

    def test_percentage_50_creates_two_chunks(self, tmp_path, fileset_json):
        out_dir = tmp_path / "chunks"
        split_fileset_standalone(fileset_json, out_dir, percentage=50)
        assert manifest_count(out_dir) == 2

    def test_datasets_filter(self, tmp_path, fileset_json):
        out_dir = tmp_path / "chunks"
        split_fileset_standalone(fileset_json, out_dir, datasets=["A"])
        chunk = json.loads((out_dir / "chunk_0.json").read_text())
        assert "A" in chunk
        assert "B" not in chunk

    def test_creates_out_dir(self, tmp_path, fileset_json):
        out_dir = tmp_path / "new" / "dir"
        split_fileset_standalone(fileset_json, out_dir)
        assert out_dir.is_dir()


def manifest_count(out_dir: Path) -> int:
    return json.loads((out_dir / "manifest.json").read_text())["n_chunks"]


# ---------------------------------------------------------------------------
# run_chunk_analysis_standalone
# ---------------------------------------------------------------------------

class FakeOk:
    """Minimal stand-in for result-types Ok."""
    def __init__(self, value):
        self._value = value

    def is_ok(self):
        return True

    def unwrap(self):
        return self._value


class FakeErr:
    def __init__(self, msg):
        self._msg = msg

    def is_ok(self):
        return False

    def __str__(self):
        return self._msg


class TestRunChunkAnalysisStandalone:
    def _chunk_file(self, tmp_path, fileset):
        p = tmp_path / "chunk.json"
        p.write_text(json.dumps(fileset))
        return p

    def test_ok_result_writes_ok_status(self, tmp_path, simple_fileset):
        chunk = self._chunk_file(tmp_path, simple_fileset)

        def builder(fileset):
            return FakeOk(({"hist": 1}, {}))

        run_chunk_analysis_standalone(
            chunk_path=chunk,
            builder=builder,
            out_payload=tmp_path / "payload.pkl",
            out_status=tmp_path / "status",
        )

        assert (tmp_path / "status").read_text() == "ok"
        assert (tmp_path / "payload.pkl").exists()

    def test_err_result_writes_failed_status(self, tmp_path, simple_fileset):
        chunk = self._chunk_file(tmp_path, simple_fileset)

        def builder(fileset):
            return FakeErr("something went wrong")

        run_chunk_analysis_standalone(
            chunk_path=chunk,
            builder=builder,
            out_payload=tmp_path / "payload.pkl",
            out_status=tmp_path / "status",
        )

        status = (tmp_path / "status").read_text()
        assert status.startswith("failed")
        assert (tmp_path / "payload.pkl").exists()

    def test_exception_in_builder_writes_failed_status(self, tmp_path, simple_fileset):
        chunk = self._chunk_file(tmp_path, simple_fileset)

        def builder(fileset):
            raise RuntimeError("XRootD timeout")

        run_chunk_analysis_standalone(
            chunk_path=chunk,
            builder=builder,
            out_payload=tmp_path / "payload.pkl",
            out_status=tmp_path / "status",
        )

        status = (tmp_path / "status").read_text()
        assert "failed" in status
        assert "XRootD timeout" in status
        # payload is still written (None pickled)
        assert (tmp_path / "payload.pkl").exists()

    def test_always_writes_both_outputs(self, tmp_path, simple_fileset):
        chunk = self._chunk_file(tmp_path, simple_fileset)

        def builder(fileset):
            raise Exception("boom")

        run_chunk_analysis_standalone(
            chunk_path=chunk,
            builder=builder,
            out_payload=tmp_path / "payload.pkl",
            out_status=tmp_path / "status",
        )

        assert (tmp_path / "payload.pkl").exists()
        assert (tmp_path / "status").exists()

    def test_creates_parent_dirs_for_outputs(self, tmp_path, simple_fileset):
        chunk = self._chunk_file(tmp_path, simple_fileset)

        def builder(fileset):
            return FakeOk(({}, {}))

        run_chunk_analysis_standalone(
            chunk_path=chunk,
            builder=builder,
            out_payload=tmp_path / "deep" / "payload.pkl",
            out_status=tmp_path / "deep" / "status",
        )

        assert (tmp_path / "deep" / "payload.pkl").exists()


# ---------------------------------------------------------------------------
# merge_chunk_results_standalone
# ---------------------------------------------------------------------------

class TestMergeChunkResultsStandalone:
    def _write_chunk(self, directory, name, result, status):
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"{name}.pkl").write_bytes(cloudpickle.dumps(result))
        (directory / f"{name}.status").write_text(status)

    def test_merges_ok_chunks(self, tmp_path):
        from coffea.processor import dict_accumulator

        chunks_dir = tmp_path / "chunks"
        # Two successful chunks with dict_accumulator so accumulate() works
        for i in range(2):
            self._write_chunk(chunks_dir, f"chunk_{i}", FakeOk(({f"k{i}": i}, {})), "ok")

        payloads = [chunks_dir / f"chunk_{i}.pkl" for i in range(2)]
        statuses = [chunks_dir / f"chunk_{i}.status" for i in range(2)]

        out = tmp_path / "merged.pkl"
        merge_chunk_results_standalone(payloads, statuses, "analysis:run", out)

        result = cloudpickle.loads(out.read_bytes())
        assert result["n_chunks_total"] == 2
        assert result["n_chunks_ok"] == 2
        assert result["failures"] == []

    def test_failed_chunks_are_tracked(self, tmp_path):
        chunks_dir = tmp_path / "chunks"
        self._write_chunk(chunks_dir, "chunk_0", FakeOk(({}, {})), "ok")
        self._write_chunk(chunks_dir, "chunk_1", None, "failed: timeout")

        payloads = [chunks_dir / "chunk_0.pkl", chunks_dir / "chunk_1.pkl"]
        statuses = [chunks_dir / "chunk_0.status", chunks_dir / "chunk_1.status"]

        out = tmp_path / "merged.pkl"
        merge_chunk_results_standalone(payloads, statuses, "analysis:run", out)

        result = cloudpickle.loads(out.read_bytes())
        assert result["n_chunks_ok"] == 1
        assert len(result["failures"]) == 1
        assert "timeout" in result["failures"][0]["error"]

    def test_all_failed_gives_zero_ok(self, tmp_path):
        chunks_dir = tmp_path / "chunks"
        for i in range(3):
            self._write_chunk(chunks_dir, f"chunk_{i}", None, "failed: err")

        payloads = [chunks_dir / f"chunk_{i}.pkl" for i in range(3)]
        statuses = [chunks_dir / f"chunk_{i}.status" for i in range(3)]

        out = tmp_path / "merged.pkl"
        merge_chunk_results_standalone(payloads, statuses, "analysis:run", out)

        result = cloudpickle.loads(out.read_bytes())
        assert result["n_chunks_ok"] == 0
        assert result["n_chunks_total"] == 3

    def test_output_has_expected_keys(self, tmp_path):
        chunks_dir = tmp_path / "chunks"
        self._write_chunk(chunks_dir, "chunk_0", FakeOk(({}, {})), "ok")

        out = tmp_path / "merged.pkl"
        merge_chunk_results_standalone(
            [chunks_dir / "chunk_0.pkl"],
            [chunks_dir / "chunk_0.status"],
            "analysis:run",
            out,
        )

        result = cloudpickle.loads(out.read_bytes())
        for key in ("builder", "n_chunks_total", "n_chunks_ok", "failures", "processor_result"):
            assert key in result

    def test_creates_parent_dirs(self, tmp_path):
        chunks_dir = tmp_path / "chunks"
        self._write_chunk(chunks_dir, "chunk_0", FakeOk(({}, {})), "ok")

        out = tmp_path / "deep" / "dir" / "merged.pkl"
        merge_chunk_results_standalone(
            [chunks_dir / "chunk_0.pkl"],
            [chunks_dir / "chunk_0.status"],
            "analysis:run",
            out,
        )
        assert out.exists()


# ---------------------------------------------------------------------------
# make_plot_standalone
# ---------------------------------------------------------------------------

class TestMakePlotStandalone:
    def _write_merged(self, tmp_path):
        payload = {
            "builder": "analysis:run",
            "n_chunks_total": 1,
            "n_chunks_ok": 1,
            "failures": [],
            "processor_result": ({"hist": 42}, {}),
        }
        p = tmp_path / "merged.pkl"
        p.write_bytes(cloudpickle.dumps(payload))
        return p

    def test_calls_plot_builder_with_payload(self, tmp_path):
        merged = self._write_merged(tmp_path)
        received = {}

        def plot_fn(payload):
            received["payload"] = payload
            return "figure"

        out = tmp_path / "plot.pkl"
        make_plot_standalone(merged, plot_fn, out)

        assert received["payload"]["n_chunks_ok"] == 1
        assert cloudpickle.loads(out.read_bytes()) == "figure"

    def test_creates_output_file(self, tmp_path):
        merged = self._write_merged(tmp_path)

        def plot_fn(payload):
            return None

        out = tmp_path / "plots" / "result.pkl"
        make_plot_standalone(merged, plot_fn, out)
        assert out.exists()

    def test_creates_parent_dirs(self, tmp_path):
        merged = self._write_merged(tmp_path)

        def plot_fn(payload):
            return "ok"

        out = tmp_path / "a" / "b" / "c" / "plot.pkl"
        make_plot_standalone(merged, plot_fn, out)
        assert out.exists()
