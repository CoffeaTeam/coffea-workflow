from __future__ import annotations

import json
import socket
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .producers_utils import _load_object, _safe_print

_HISTSERV_SITE_ADDRESSES = {
    "nebraska": "histserv.cmsaf-dev.flatiron.hollandhpc.org:8788",
    "uchicago": "histserv.coffea-casa-dev:8788",
}
_HISTSERV_SITE_SIGNALS = {
    "nebraska": ("unl.edu",),
    "uchicago": ("uchicago.edu",),
}

_CONNECTION_INFO_FILENAME = "histserv_connection_info.json"


def detect_histserv_address(override: str | None = None) -> str:
    """
    Resolve the histserv address for the coffea-casa site this code is running on.

    Pass `override` to skip detection and use an explicit address (local server,
    a site not yet recognized here, etc.) — it always wins over detection.

    Detection is a heuristic, not a documented API: it looks for a site-identifying
    substring in /etc/resolv.conf's search domains, hostname, and fqdn. If it can't
    determine the site (running outside coffea-casa, or an unrecognized deployment),
    it raises rather than guessing — pass the address explicitly in that case.
    """
    if override:
        return override

    try:
        resolv = Path("/etc/resolv.conf").read_text()
    except OSError:
        resolv = ""
    haystack = "\n".join([resolv, socket.gethostname(), socket.getfqdn()]).lower()

    for site, signals in _HISTSERV_SITE_SIGNALS.items():
        if any(signal in haystack for signal in signals):
            address = _HISTSERV_SITE_ADDRESSES[site]
            _safe_print(f"Detected coffea-casa site: {site!r} -> histserv address {address!r}")
            return address

    raise RuntimeError(
        "Could not auto-detect the coffea-casa site to pick a histserv address "
        f"(checked /etc/resolv.conf, hostname, and fqdn for {sorted(_HISTSERV_SITE_SIGNALS)}). "
        "Pass the address explicitly instead: detect_histserv_address(override='host:port')."
    )


def _connection_info_path(out_dir: Path) -> Path:
    return out_dir / _CONNECTION_INFO_FILENAME


def _try_reconnect(hist_client: Any, connection_info: dict) -> bool:
    """
    Cheap validity probe (a Describe RPC, no data transfer): True if the histogram
    is still live on the server, False if it's gone (e.g. pruned after inactivity —
    histserv has no expiry timestamp in its API, so this is the only way to find out).
    """
    import grpc
    try:
        hist_client.connect(hist_id=connection_info["hist_id"], token=connection_info.get("token"))
        return True
    except grpc.RpcError:
        return False


def _create_histogram(hist_client: Any, hist_template: "str | Callable", token: str | None) -> tuple[dict, str]:
    fn = _load_object(hist_template)
    remote_hist = hist_client.init(hist=fn(), token=token)
    created_at = datetime.now(timezone.utc).isoformat()
    return remote_hist.get_connection_info(), created_at


def resolve_histserv_connection(
    *,
    hist_client: Any,
    hist_template: "str | Callable",
    histserv_token: str | None,
    provided_connection_info: dict | None,
    out_dir: Path,
) -> dict:
    """
    Ensure a valid histserv connection for one Analysis artifact and return its
    connection_info. The framework, not the user, owns reconnect vs. recreate:

      - a user-provided histserv_connection_info, or one cached on disk from a prior
        run of this exact Analysis identity, is validated with a cheap reconnect probe
      - if that fails (the server pruned it after inactivity — histserv's default is
        24h idle, but the actual server config may differ and isn't queryable) or
        none exists yet, a new histogram is created via hist_template() and persisted
        alongside the artifact so the next run reconnects automatically

    Prints what happened either way, so a silent auto-recreate never hides a
    discontinuity (results before/after are in a different remote histogram).
    """
    conn_path = _connection_info_path(out_dir)

    candidate = provided_connection_info
    candidate_source = "manually provided"
    if candidate is None and conn_path.exists():
        cached = json.loads(conn_path.read_text())
        candidate = cached["connection_info"]
        candidate_source = f"cached, created {cached['created_at']}"

    if candidate is not None:
        if _try_reconnect(hist_client, candidate):
            _safe_print(
                f"Reconnected to histserv: hist_id={candidate['hist_id']!r} "
                f"address={candidate['address']!r} ({candidate_source})"
            )
            return candidate
        _safe_print(
            f"Previous histserv histogram hist_id={candidate['hist_id']!r} ({candidate_source}) "
            "is no longer reachable on the server — likely pruned after inactivity. "
            "Creating a new histogram; earlier and later results now live in different remote histograms."
        )

    new_conn, created_at = _create_histogram(hist_client, hist_template, histserv_token)
    out_dir.mkdir(parents=True, exist_ok=True)
    conn_path.write_text(json.dumps({"connection_info": new_conn, "created_at": created_at}, indent=2))
    _safe_print(
        f"Created histserv histogram: hist_id={new_conn['hist_id']!r} "
        f"address={new_conn['address']!r} at {created_at}"
    )
    return new_conn
