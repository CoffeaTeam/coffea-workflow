"""
Compatibility tests against the installed coffea release.

coffea-workflow depends on a specific slice of coffea's API, most notably the
Ok/Err result-type protocol behind ``processor.Runner(use_result_type=True)``.
This module pins down that contract so a new coffea release that changes or
removes any of it fails loudly here rather than deep inside a producer.

Run against the latest coffea by the scheduled coffea-compat workflow, and
against the pinned minimum in regular CI.
"""
import inspect

import numpy as np
import pytest


# ---------------------------------------------------------------------------
# API surface: every coffea symbol the engine imports must exist
# ---------------------------------------------------------------------------

class TestApiSurface:
    def test_processor_symbols(self):
        from coffea.processor import (  # noqa: F401
            accumulate,
            IterativeExecutor,
            FuturesExecutor,
            DaskExecutor,
            Runner,
            ProcessorABC,
        )

    def test_result_type_symbols(self):
        from coffea.processor import Ok, Err  # noqa: F401

    def test_dataset_tools_symbols(self):
        from coffea.dataset_tools.splitting import (  # noqa: F401
            split_fileset,
            hash_fileset,
        )

    def test_runner_accepts_use_result_type(self):
        from coffea.processor import Runner

        assert "use_result_type" in inspect.signature(Runner).parameters


# ---------------------------------------------------------------------------
# Result-type protocol: what the producers rely on
# ---------------------------------------------------------------------------

class TestResultProtocol:
    def test_ok_protocol(self):
        from coffea.processor import Ok

        ok = Ok(41)
        assert ok.is_ok() is True
        assert ok.unwrap() == 41

    def test_err_protocol(self):
        from coffea.processor import Err

        err = Err(RuntimeError("boom"))
        assert err.is_ok() is False
        # producers record failures via str(result)
        assert str(err)


# ---------------------------------------------------------------------------
# End-to-end: Runner(use_result_type=True) over a real (tiny) ROOT file
# ---------------------------------------------------------------------------

class _CountProcessor:
    """Minimal processor counting events, savemetrics-compatible."""

    def __init__(self):
        from coffea import processor

        self.output = processor.dict_accumulator(
            {"cutflow": processor.defaultdict_accumulator(int)}
        )

    def process(self, events):
        self.output["cutflow"]["all events"] += len(events)
        return self.output

    def postprocess(self, accumulator):
        pass


@pytest.fixture(scope="module")
def tiny_fileset(tmp_path_factory):
    uproot = pytest.importorskip("uproot")
    path = tmp_path_factory.mktemp("data") / "tiny.root"
    with uproot.recreate(path) as f:
        f["Events"] = {"x": np.arange(100.0)}
    return {"tiny": {"files": {str(path): "Events"}}}


class TestRunnerEndToEnd:
    def _run(self, fileset):
        from coffea import processor
        from coffea.nanoevents import schemas
        from coffea.processor import IterativeExecutor

        run = processor.Runner(
            executor=IterativeExecutor(),
            schema=schemas.BaseSchema,
            savemetrics=True,
            skipbadfiles=True,  # required by use_result_type: defines what becomes Err
            use_result_type=True,
        )
        # ProcessorABC subclassing is checked structurally by Runner; build a
        # real subclass here to match user analysis code.
        from coffea.processor import ProcessorABC

        class Proc(_CountProcessor, ProcessorABC):
            pass

        return run(fileset, Proc())

    def test_ok_result_with_metrics_tuple(self, tiny_fileset):
        from coffea_workflow.producers_utils import _extract_acc

        result = self._run(tiny_fileset)
        assert result.is_ok() is True

        # engine contract: unwrap() yields (acc, metrics) with savemetrics=True
        acc, metrics = _extract_acc(result)
        assert acc["cutflow"]["all events"] == 100

    def test_result_survives_pickle_roundtrip(self, tiny_fileset):
        # chunk payloads are cloudpickled to the cache and reloaded
        import cloudpickle

        result = self._run(tiny_fileset)
        restored = cloudpickle.loads(cloudpickle.dumps(result))
        assert restored.is_ok() is True


# ---------------------------------------------------------------------------
# split_fileset / hash_fileset behave as the producers expect
# ---------------------------------------------------------------------------

class TestSplittingContract:
    FILESET = {
        "A": {"files": {"a1.root": "Events", "a2.root": "Events"}},
        "B": {"files": {"b1.root": "Events"}},
    }

    def test_split_by_dataset(self):
        from coffea.dataset_tools.splitting import split_fileset

        chunks = split_fileset(self.FILESET, strategy="by_dataset")
        assert len(chunks) == 2
        assert {tuple(c.keys()) for c in chunks} == {("A",), ("B",)}

    def test_hash_fileset_is_deterministic(self):
        from coffea.dataset_tools.splitting import hash_fileset

        assert hash_fileset(self.FILESET) == hash_fileset(self.FILESET)
