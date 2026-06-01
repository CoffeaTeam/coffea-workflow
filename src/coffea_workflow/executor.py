from __future__ import annotations
from pathlib import Path
from typing import Any, Type

from .artifacts import Artifact
from .producers import get_producer
from .deps import Deps
from .config import RunConfig

class Executor:
    """
    Executor must materialise the artifacts applying same configurations defined for the whole workflow.
    It knows RunConfig(with splitting strategy and percentage) and cache directory.
    """    
    def __init__(self, cache_dir: Path, config: RunConfig):
        self.cache_dir = cache_dir
        self.config = config
        self._session_cache: set[Path] = set()  # paths materialized this run
        self._coffea_executor: Any = None  # pass same coffea executor to different chunks if split strategy is applied instead of creating multiple
    

    def path_for(self, art: Artifact) -> Path:
        """
        Fileset:
            .cache/Fileset/<identity>/fileset.json
        
        Chunking:
            .cache/Chunking/<identity>/ 
                manifest.json
                fileset_datasetA.json
                fileset_datasetB.json
            OR
            .cache/Chunking/<identity>/
                manifest.json
                fileset_0_20_percent.json
                fileset_20_40_percent.json
                ...
        
        ChunkAnalysis:
            .cache/ChunkAnalysis/<chunk_identity>/payload.pkl
                                /<chunk_identity>/payload.pkl
                                /<chunk_identity>/payload.pkl
        
        Analysis (merged analysis):
            .cache/Analysis/<analysis_identity>/payload.pkl
        """
        return self.cache_dir / art.type_name / art.identity() 

    def get_coffea_executor(self, config: RunConfig) -> Any:
        """
        Build the coffea executor on first call and reuse it for all chunks.
        """
        if self._coffea_executor is None:
            from .producers_utils import build_executor
            self._coffea_executor = build_executor(config.executor_config, config.facility)
        return self._coffea_executor

    _EXPECTED = {
        "Fileset": "fileset.json",
        "Chunking": "manifest.json",
        "ChunkAnalysis": ".success",
        "Analysis": "payload.pkl",
        "Plotting": "payload.pkl",
        "CustomArtifact": "payload.pkl", 
    }

    def exists(self, art: Artifact, config: RunConfig | None = None) -> bool:
        effective_config = config if config is not None else self.config
        out = self.path_for(art)
        if not out.is_dir():
            return False
        expected = self._EXPECTED.get(art.type_name)
        if expected and not (out / expected).exists():
            return False

        if art.type_name == "Analysis":
            # Analysis with recorded failures is not considered complete
            if (out / ".has_failures").exists():
                return False
            # if chunk_fraction has changed since this result was cached
            stamp = out / ".chunk_fraction"
            stored = stamp.read_text() if stamp.exists() else "None"
            if stored != str(effective_config.chunk_fraction):
                return False
        return True

    def materialize(self, art: Artifact, config: RunConfig | None = None) -> Path:
        effective_config = config if config is not None else self.config
        out = self.path_for(art)
        if out in self._session_cache:
            return out
        if not getattr(art, "always_rerun", False) and self.exists(art, config=effective_config):
            self._session_cache.add(out)
            print(f"Extracted from cache: {out}")
            return out

        fn = get_producer(type(art))
        deps = Deps(self, config=effective_config)
        fn(art=art, deps=deps, out=out, config=effective_config)

        if not out.exists():
            raise RuntimeError(
                f"Producer for {art.type_name} finished but did not create output at {out}"
            )
        self._session_cache.add(out)
        return out