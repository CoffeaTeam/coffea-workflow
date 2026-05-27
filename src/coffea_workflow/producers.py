from __future__ import annotations
from typing import Any, Callable, Dict, List, Type

from .artifacts import Artifact

# just a function that takes (artifact, deps) and returns something
ProducerFn = Callable[[Artifact, List[Artifact]], Any]

_PRODUCERS: Dict[Type[Artifact], ProducerFn] = {}

def producer(artifact_type: Type) -> Callable[[ProducerFn], ProducerFn]:
    """
    Example: @producer(Fileset) registers the function for Fileset.
    """
    def deco(fn: ProducerFn) -> ProducerFn:
        _PRODUCERS[artifact_type] = fn
        return fn
    return deco


def get_producer(artifact_type: Type) -> ProducerFn:
    if artifact_type not in _PRODUCERS:
        raise KeyError(
            f"No producer registered for {artifact_type.__name__}. "
            f"Available artifacts: {list(_PRODUCERS.keys())}"
        )
    return _PRODUCERS[artifact_type]