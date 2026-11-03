"""Plot the experiment results into results/*.png (for the report and slides).

  python scripts/plot_results.py
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
RES = ROOT / "results"

SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
RESCUE, BASE = "#2a78d6", "#eb6834"  # categorical slots 1 and 2
LABELS = {"baseline": "Baseline (no fault tolerance)", "semantic_rescue": "SemanticRescue"}

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "text.color": INK, "font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
})


def read_csv(p):
    with open(p, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def grouped_barh(rows, field, title, xlabel, out, xmax=None, fmt="{:.0f}"):
    names = list(dict.fromkeys(r["scenario"] for r in rows))
    get = {(r["scenario"], r["mode"]): float(r[field]) for r in rows if r[field] not in ("", "None")}
    fig, ax = plt.subplots(figsize=(8, 0.48 * len(names) + 1.4))
    h = 0.36
    for j, (mode, color) in enumerate((("baseline", BASE), ("semantic_rescue", RESCUE))):
        ys = [i + (h / 2 + 0.02) * (-1 if j == 0 else 1) for i in range(len(names))]
        vals = [get.get((n, mode), 0.0) for n in names]
        ax.barh(ys, vals, height=h, color=color, label=LABELS[mode])
        for y, v in zip(ys, vals):
            ax.text(v, y, " " + fmt.format(v), va="center", fontsize=8, color=INK2)
    ax.set_yticks(range(len(names)))
    ax.set_yticklabels([n.split("_", 1)[1].replace("_", " ") for n in names])
    ax.invert_yaxis()
    ax.grid(axis="y", visible=False)
    if xmax:
        ax.set_xlim(0, xmax)
    ax.set_xlabel(xlabel)
    ax.set_title(title, loc="left", fontsize=11, color=INK, pad=24)
    ax.legend(loc="lower left", bbox_to_anchor=(0, 1.0), ncol=2, frameon=False, fontsize=9,
              borderaxespad=0.2)
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def timeline(raw_path, scenario, out):
    rows = [json.loads(line) for line in open(raw_path, encoding="utf-8")]
    fig, ax = plt.subplots(figsize=(8, 3.2))
    for mode, color in (("baseline", BASE), ("semantic_rescue", RESCUE)):
        sub = [r for r in rows if r["scenario"] == scenario and r["mode"] == mode]
        ax.plot([r["idx"] for r in sub], [r["latency_s"] * 1000 for r in sub], color=color,
                linewidth=2, marker="o", markersize=3.5, label=LABELS[mode])
    ax.set_xlabel("query number after the failure starts")
    ax.set_ylabel("response time (ms)")
    ax.set_title(f"Response time per query, {scenario.split('_', 1)[1].replace('_', ' ')}", loc="left",
                 fontsize=11, pad=24)
    ax.legend(loc="lower left", bbox_to_anchor=(0, 1.0), ncol=2, frameon=False, fontsize=9, borderaxespad=0.2)
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def sync_plots(rows, out):
    rows = sorted(rows, key=lambda r: (r["push_on_write"] == "True", float(r["interval_s"])))
    labels = [f"{float(r['interval_s']):g} s" + (" + push\non write" if r["push_on_write"] == "True" else "")
              for r in rows]
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.4))
    for ax, field, title, unit in (
        (axes[0], "stale_pct", "Updates lost when the edge crashes", "stale answers (%)"),
        (axes[1], "sync_req_per_min", "Idle sync traffic of the edge", "requests per minute (no writes)"),
    ):
        vals = [float(r[field]) for r in rows]
        ax.bar(range(len(vals)), vals, color=RESCUE, width=0.55, label="measured")
        for i, v in enumerate(vals):
            ax.text(i, v, f"{v:.0f}", ha="center", va="bottom", fontsize=8, color=INK2)
        if field == "stale_pct" and "expected_stale_pct" in rows[0]:
            exp = [float(r["expected_stale_pct"]) for r in rows]
            ax.scatter(range(len(exp)), exp, marker="_", s=420, linewidths=2, color=INK, zorder=3,
                       label="expected = window / 8")
            ax.legend(frameon=False, fontsize=8, loc="upper left")
        ax.set_xticks(range(len(vals)))
        ax.set_xticklabels(labels, fontsize=8)
        ax.set_xlabel("sync window")
        ax.set_ylabel(unit)
        ax.grid(axis="x", visible=False)
        ax.set_title(title, loc="left", fontsize=11)
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def main():
    made = []
    e1 = RES / "e1_summary.csv"
    if e1.exists():
        rows = read_csv(e1)
        grouped_barh(rows, "complete_pct", "Queries answered completely", "% of queries", RES / "fig_e1_complete.png", 112)
        grouped_barh(rows, "parts_correct_pct", "Sub-queries answered correctly", "% of sub-queries",
                     RES / "fig_e1_correct.png", 112)
        grouped_barh(rows, "p95_ms", "95th percentile response time", "milliseconds", RES / "fig_e1_p95.png")
        timeline(RES / "e1_raw.jsonl", "S2_edge_partition", RES / "fig_e1_timeline_edge_partition.png")
        made += ["fig_e1_complete.png", "fig_e1_correct.png", "fig_e1_p95.png", "fig_e1_timeline_edge_partition.png"]
    e2 = RES / "e2_summary.csv"
    if e2.exists():
        sync_plots(read_csv(e2), RES / "fig_e2_sync_window.png")
        made.append("fig_e2_sync_window.png")
    if not made:
        sys.exit("no results found; run scripts/run_experiments.py first")
    print("wrote", ", ".join(made))


if __name__ == "__main__":
    main()
