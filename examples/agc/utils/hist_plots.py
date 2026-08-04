from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
from utils.plotting import set_style
import hist



def _save_fig(fig: mpl.figure.Figure, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def _write_index(index_path: Path, merged_path: str, entries: List[Tuple[str, Path]]) -> None:
    index_path.parent.mkdir(parents=True, exist_ok=True)

    lines: List[str] = []
    lines.append("# Plots\n")
    lines.append(f"- merged: `{merged_path}`\n")
    lines.append("\n## Gallery\n")

    for title, p in entries:
        rel = p.relative_to(index_path.parent)
        lines.append(f"\n### {title}\n")
        lines.append(f"![]({rel.as_posix()})\n")
        lines.append(f"`{p.name}`\n")

    index_path.write_text("\n".join(lines))


def _get_hist_payload(payload: Any) -> Dict[str, Any]:
    """
    payload is expected to be what you store in merged.pkl.
    In your validator you used payload["hist_dict"].
    """
    if not isinstance(payload, dict):
        raise TypeError(f"Expected merged payload to be dict-like, got {type(payload)}")
    if "hist_dict" not in payload:
        raise KeyError(f"'hist_dict' not found in payload keys: {list(payload.keys())}")
    return payload


# ----------------------------
# Plots (AGC example Inspecting the produced histograms)
# ----------------------------
def plot_region_stack_4j1b_nominal(all_histograms: Dict[str, Any], *, out_path: Path,) -> Path:

    h = all_histograms["hist_dict"]["4j1b"][120j::hist.rebin(2), :, "nominal"]

    fig = plt.figure()
    h.stack("process")[::-1].plot(stack=True, histtype="fill", linewidth=1, edgecolor="grey")
    plt.legend(frameon=False)
    plt.title(r"$\geq$ 4 jets, 1 b-tag")
    plt.xlabel(r"$H_T$ [GeV]")

    _save_fig(fig, out_path)
    return out_path


def plot_region_stack_4j2b_nominal(all_histograms: Dict[str, Any], *, out_path: Path,) -> Path:
    
    h = all_histograms["hist_dict"]["4j2b"][:, :, "nominal"]

    fig = plt.figure()
    h.stack("process")[::-1].plot(stack=True, histtype="fill", linewidth=1, edgecolor="grey")
    plt.legend(frameon=False)
    plt.title(r"$\geq$ 4 jets, $\geq$ 2 b-tags")
    plt.xlabel(r"$m_{bjj}$ [GeV]")

    _save_fig(fig, out_path)
    return out_path


def plot_btag_variations_4j1b_ttbar(all_histograms: Dict[str, Any],*,out_path: Path,) -> Path:
    hreg = all_histograms["hist_dict"]["4j1b"]

    fig = plt.figure()

    hreg[120j::hist.rebin(2), "ttbar", "nominal"].plot(label="nominal", linewidth=2)
    hreg[120j::hist.rebin(2), "ttbar", "btag_var_0_up"].plot(label="NP 1", linewidth=2)
    hreg[120j::hist.rebin(2), "ttbar", "btag_var_1_up"].plot(label="NP 2", linewidth=2)
    hreg[120j::hist.rebin(2), "ttbar", "btag_var_2_up"].plot(label="NP 3", linewidth=2)
    hreg[120j::hist.rebin(2), "ttbar", "btag_var_3_up"].plot(label="NP 4", linewidth=2)

    plt.legend(frameon=False)
    plt.xlabel(r"$H_T$ [GeV]")
    plt.title("b-tagging variations")

    _save_fig(fig, out_path)
    return out_path


def plot_jet_energy_variations_4j2b_ttbar(all_histograms: Dict[str, Any],*,out_path: Path,) -> Path:

    hreg = all_histograms["hist_dict"]["4j2b"]

    fig = plt.figure()

    hreg[:, "ttbar", "nominal"].plot(label="nominal", linewidth=2)
    hreg[:, "ttbar", "pt_scale_up"].plot(label="scale up", linewidth=2)
    hreg[:, "ttbar", "pt_res_up"].plot(label="resolution up", linewidth=2)

    plt.legend(frameon=False)
    plt.xlabel(r"$m_{bjj}$ [GeV]")
    plt.title("Jet energy variations")

    _save_fig(fig, out_path)
    return out_path


def plot_ml_inference_grid(all_histograms: Dict[str, Any],*,feature_names: List[str],out_path: Path,) -> Path:

    ml_hist_dict = all_histograms.get("ml_hist_dict")
    if ml_hist_dict is None:
        raise KeyError("ml_hist_dict not present in merged payload")

    n = len(feature_names)
    nrows = 10
    ncols = 2
    fig, axs = plt.subplots(nrows, ncols, figsize=(14, 40))

    for i, feat in enumerate(feature_names):
        col = 0 if i < 10 else 1
        row = i if i < 10 else i - 10
        if row >= nrows:
            break  # keep the grid size fixed like the example

        ax = axs[row, col]
        h = ml_hist_dict[feat][:, :, "nominal"].stack("process").project("observable")
        h.plot(stack=True, histtype="fill", linewidth=1, edgecolor="grey", ax=ax)
        ax.legend(frameon=False)
        ax.set_title(feat)

    _save_fig(fig, out_path)
    return out_path



def make_all_agc_example_plots(
    *,
    payload: Any,
    merged_path: str,
    plot_dir: str | Path,
    plot_index: str | Path,
    use_inference: bool = False,
    feature_names: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    One entry point that produces all example plots and writes an index.md.
    Returns a small summary dict.
    """
    all_histograms = _get_hist_payload(payload)

    plot_dir = Path(plot_dir)
    plot_index = Path(plot_index)
    plot_dir.mkdir(parents=True, exist_ok=True)
    plot_index.parent.mkdir(parents=True, exist_ok=True)

    set_style()

    entries: List[Tuple[str, Path]] = []

    p1 = plot_region_stack_4j1b_nominal(all_histograms, out_path=plot_dir / "4j1b_nominal_stack.png")
    entries.append(("≥ 4 jets, 1 b-tag (nominal stacked)", p1))

    p2 = plot_region_stack_4j2b_nominal(all_histograms, out_path=plot_dir / "4j2b_nominal_stack.png")
    entries.append(("≥ 4 jets, ≥ 2 b-tags (nominal stacked)", p2))

    p3 = plot_btag_variations_4j1b_ttbar(all_histograms, out_path=plot_dir / "4j1b_ttbar_btag_variations.png")
    entries.append(("4j1b ttbar: b-tagging variations", p3))

    p4 = plot_jet_energy_variations_4j2b_ttbar(all_histograms, out_path=plot_dir / "4j2b_ttbar_jet_energy_variations.png")
    entries.append(("4j2b ttbar: jet energy variations", p4))

    if use_inference:
        if not feature_names:
            raise ValueError("use_inference=True but feature_names is empty")
        p5 = plot_ml_inference_grid(
            all_histograms,
            feature_names=feature_names,
            out_path=plot_dir / "ml_inference_grid.png",
        )
        entries.append(("ML inference variables (nominal stacked)", p5))

    _write_index(plot_index, merged_path, entries)

    return {
        "ok": True,
        "n_plots": len(entries),
        "files": [str(p) for _, p in entries],
        "index": str(plot_index),
    }
