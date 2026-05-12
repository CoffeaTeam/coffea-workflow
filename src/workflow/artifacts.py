from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol, runtime_checkable
from .identity import hash_identity


def _builder_key(builder: str | Callable) -> str:
    """
    Returns a stable string key for a builder (string or callable).
    Functions are serialised as 'module:qualname' so identity is deterministic.
    """
    if callable(builder):
        return f"{builder.__module__}:{builder.__qualname__}"
    return builder

def _to_params_tuple(builder_params) -> tuple:
    """Convert dict builder_params to a sorted tuple of items for frozen-dataclass storage."""
    if not builder_params:
        return ()
    if isinstance(builder_params, dict):
        return tuple(sorted(builder_params.items()))
    return tuple(builder_params)

ARTIFACT_REGISTRY = {}

def register_artifact(cls):
    ARTIFACT_REGISTRY[cls.__name__] = cls
    return cls

@runtime_checkable
class Artifact(Protocol):
    def keys(self) -> Mapping[str, Any]: ...
    def identity(self) -> str: ...
    def to_dict(self) -> dict: ...
    @property
    def type_name(self) -> str: ...

@dataclass(frozen=True)
class ArtifactBase:
    always_rerun = False

    def keys(self):
        raise NotImplementedError

    @property
    def type_name(self) -> str:
        return type(self).__name__

    def identity(self) -> str:
        return hash_identity(self.to_dict())

    def to_dict(self) -> dict:
        return {"type": self.__class__.__name__, "keys": self.keys()}

@register_artifact
@dataclass(frozen=True)
class Fileset(ArtifactBase):
    """
    External artifact that user uses and defines the builder(his/her cutom script that returns fileset.json)
    """
    name: str
    builder: str | Callable
    builder_params: tuple = ()
     
    def __post_init__(self):
        object.__setattr__(self, 'builder_params', _to_params_tuple(self.builder_params))
 
    def keys(self) -> Mapping[str, Any]:
        return {
            "name": self.name,
            "builder": _builder_key(self.builder),
            "builder_params": dict(self.builder_params),
        }

@register_artifact
@dataclass(frozen=True)
class Chunking(ArtifactBase):
    """
    Internal artifact. User doesn't use this artifact, it's a helping artifact that is
    produced by Analysis artifact(external) to execute splitting strategy.
    Returns fileset chunks based on splitting strategy.
    Its producer writes:
        .cache/Chunking/<identity>/
            manifest.json
            fileset_chunk_0.json
            fileset_chunk_1.json
            ...
    """
    fileset: ArtifactBase
    split_strategy: str | None
    percentage: int | None
    datasets: tuple[str, ...] | None = None

    def keys(self):
        return {
            "fileset": self.fileset,
            "split_strategy": self.split_strategy,
            "percentage": self.percentage,
            "datasets": self.datasets,
        }

@register_artifact
@dataclass(frozen=True)
class ChunkAnalysis(ArtifactBase):
    """
    Internal artifact. User doesn't use this artifact, it's a helping artifact that is produced 
    by Analysis artifact to process one chunk.
    Its producer's output example:
        - coffea acc (this should be the outcome of user specified builder)
        - .cache/ChunkAnalysis/<chunk_identity>/payload.pkl
    This file is immediately merged to the previous chunk coffea accumulator in Analysis.
    If analysis breaks then it doesn't create an output for that chunk. But Analysis artifact continues the processing of the rest.
    And will identify the missing one when rerunning analysis again.
    """
    chunk_file: str
    chunking: Chunking
    analysis_builder: str | Callable
    builder_params: tuple = ()
     
    def __post_init__(self):
        object.__setattr__(self, 'builder_params', _to_params_tuple(self.builder_params))



    def keys(self):
        return {
            "chunk_file": self.chunk_file,
            "chunking": self.chunking,
            "analysis_builder": _builder_key(self.analysis_builder),
            "builder_params": dict(self.builder_params),
        }

@register_artifact
@dataclass(frozen=True)
class Analysis(ArtifactBase):
    """
    External artifact. Its producer triggers internal Artifact materialization
    of Chunking artifact first. Iterates through gotten chunks and perfoms ChunkAnalysis.
    Ignores failers per chunk but tracks failed chunks.
    Merges whatever was successfully processed in ChunkAnalysis into one output. 
    Returns merged output and report failed chunks.
    - .cache/Analysis/<analysis_identity>/payload.pkl (with merged output)
    """
    name: str
    fileset: ArtifactBase
    builder: str | Callable
    builder_params: tuple = ()

    def __post_init__(self):
        object.__setattr__(self, 'builder_params', _to_params_tuple(self.builder_params))


    def keys(self):
        return {
            "name": self.name,
            "fileset": self.fileset,
            "builder": _builder_key(self.builder),
            "builder_params": dict(self.builder_params),
        }

@register_artifact
@dataclass(frozen=True)
class Plotting(ArtifactBase):
    always_rerun = True

    name: str
    analysis: ArtifactBase
    builder: str | Callable
    builder_params: tuple = ()
     
    def __post_init__(self):
        object.__setattr__(self, 'builder_params', _to_params_tuple(self.builder_params))

    def keys(self):
        return {
            "name": self.name,
            "analysis": self.analysis,
            "builder": _builder_key(self.builder),
            "builder_params": dict(self.builder_params),
        }

@register_artifact
@dataclass(frozen=True)
class CustomArtifact(ArtifactBase):
    name: str
    builder: str | Callable
    builder_params: tuple = ()
    upstreams: tuple = ()
    identity_keys: tuple = ()

    def __post_init__(self):
        object.__setattr__(self, 'builder_params', _to_params_tuple(self.builder_params))
        object.__setattr__(self, 'upstreams', tuple(self.upstreams))
        object.__setattr__(self, 'identity_keys', tuple(self.identity_keys))

    def _all_keys(self):
        return {
            "name": self.name,
            "builder": _builder_key(self.builder),
            "builder_params": dict(self.builder_params),
            "upstreams": list(self.upstreams),
        }

    def keys(self):
        all_keys = self._all_keys()
        return all_keys