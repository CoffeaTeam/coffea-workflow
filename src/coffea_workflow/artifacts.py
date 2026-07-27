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


def _normalize_for_identity(v):
    """
    Recursively convert a value into a JSON/hash-stable representation for cache identity.

    processor_params/runner_params commonly hold things json.dumps can't handle directly
    (e.g. schema=NanoAODSchema): classes/functions become 'module:qualname' strings (see
    _builder_key), containers are normalized recursively, scalars pass through unchanged.
    """
    if isinstance(v, type) or callable(v):
        return _builder_key(v)
    if isinstance(v, dict):
        return {k: _normalize_for_identity(val) for k, val in v.items()}
    if isinstance(v, (list, tuple)):
        return [_normalize_for_identity(x) for x in v]
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    from pathlib import Path
    if isinstance(v, Path):
        return str(v)
    raise TypeError(
        f"Value of type {type(v).__name__} is not identity-stable for caching. "
        "Use a class/function, string, number, bool, None, Path, dict, or list/tuple."
    )


def _identity_safe_params(params_tuple: tuple) -> dict:
    """Normalize a (k, v) params tuple into a dict safe for cache-identity hashing."""
    return {k: _normalize_for_identity(v) for k, v in params_tuple}

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
    input_type  = "any"
    output_type = "any"

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
    
    input_type  = "none"
    output_type = "fileset_dict"
    
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

    Mirrors Analysis's two modes: either analysis_builder (function, escape hatch) or
    processor (declarative — framework builds the coffea Runner) is set, never both.
    """
    chunk_file: str
    chunk_hash: str
    chunking: Chunking
    analysis_builder: str | Callable | None = None
    builder_params: tuple = ()
    processor: str | Callable | None = None
    processor_params: tuple = ()
    runner_params: tuple = ()

    def __post_init__(self):
        object.__setattr__(self, 'builder_params', _to_params_tuple(self.builder_params))
        object.__setattr__(self, 'processor_params', _to_params_tuple(self.processor_params))
        object.__setattr__(self, 'runner_params', _to_params_tuple(self.runner_params))
        if (self.analysis_builder is None) == (self.processor is None):
            raise ValueError(
                "ChunkAnalysis needs exactly one of 'analysis_builder' or 'processor'."
            )

    def keys(self):
        return {
            "chunk_hash": self.chunk_hash,
            "analysis_builder": _builder_key(self.analysis_builder) if self.analysis_builder is not None else None,
            "builder_params": dict(self.builder_params),
            "processor": _builder_key(self.processor) if self.processor is not None else None,
            "processor_params": _identity_safe_params(self.processor_params),
            "runner_params": _identity_safe_params(self.runner_params),
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

    Two mutually-exclusive ways to define the analysis — set exactly one:
      - builder: 'module:function' (or a callable) that receives the fileset (and
        optionally executor/config) and drives its own coffea Runner. Escape hatch
        for custom orchestration.
      - processor: 'module:Class' (or a class) naming a coffea ProcessorABC subclass.
        The framework builds the Runner itself: processor_params construct the
        Processor (Processor(**processor_params)), runner_params are passed through
        to processor.Runner(**runner_params). The framework always injects the
        executor and forces use_result_type=True.
    """
    input_type  = "fileset_dict"
    output_type = "analysis_payload"

    name: str
    fileset: ArtifactBase
    builder: str | Callable | None = None
    builder_params: tuple = ()
    processor: str | Callable | None = None
    processor_params: tuple = ()
    runner_params: tuple = ()


    def __post_init__(self):
        object.__setattr__(self, 'builder_params', _to_params_tuple(self.builder_params))
        object.__setattr__(self, 'processor_params', _to_params_tuple(self.processor_params))
        object.__setattr__(self, 'runner_params', _to_params_tuple(self.runner_params))
        if (self.builder is None) == (self.processor is None):
            raise ValueError(
                f"Analysis step '{self.name}' needs exactly one of 'builder' (function, "
                "escape hatch) or 'processor' (declarative Processor class) — got "
                f"builder={self.builder!r}, processor={self.processor!r}."
            )


    def keys(self):
        return {
            "name": self.name,
            "fileset": self.fileset,
            "builder": _builder_key(self.builder) if self.builder is not None else None,
            "builder_params": dict(self.builder_params),
            "processor": _builder_key(self.processor) if self.processor is not None else None,
            "processor_params": _identity_safe_params(self.processor_params),
            "runner_params": _identity_safe_params(self.runner_params),
        }

@register_artifact
@dataclass(frozen=True)
class Plotting(ArtifactBase):
    always_rerun = True
    input_type   = "analysis_payload"
    output_type  = "plot_result"
    
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