from __future__ import annotations
import hashlib
import json
from typing import Any


def canonicalize(obj: Any) -> bytes:
    def default(o):
        if hasattr(o, "to_dict"):
            return o.to_dict()
            
        try:
            from pathlib import Path
            if isinstance(o, Path):
                return str(o)
        except Exception:
            pass
        raise TypeError(f"Not JSON serializable: {type(o)}")

    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=default,
    ).encode("utf-8")

def hash_identity(*parts: Any) -> str:
    """TODO change hashing approach"""
    h = hashlib.sha256()
    for p in parts:
        if isinstance(p, (bytes, bytearray)):
            h.update(p)
        else:
            h.update(canonicalize(p))
        h.update(b"|")
    return h.hexdigest()
