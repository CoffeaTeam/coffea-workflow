from __future__ import annotations
from pathlib import Path
from .artifacts import Artifact

class Deps:
    """
    Dependencies are initialized in the executor that knows how to materilize the artifact(it knows config and cache directory).
    The producer of the specific artifact then trigers the same executor to produce the previous dependency (artifact).
    Carries the per-step config so nested artifacts (Chunking, ChunkAnalysis) inherit it.
    """
    def __init__(self, executor, config=None):
        self._executor = executor
        self._config = config

    def need(self, art: Artifact) -> Path:
        # triggers dependency marelization
        return self._executor.materialize(art, config=self._config)
