"""
benchmarks/compare_stages.py

Reads all available benchmark JSON files and prints a side-by-side
comparison across all three storage formats. Works with whatever files
exist — shows PENDING for stages not yet run, so this can be re-run
after each stage without erroring.

Run this after each stage to see the cumulative improvement story build
up. Final run (after Stage 3) produces the project's headline numbers.
"""

import argparse
import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("compare_stages")

BENCHMARKS_DIR = Path("benchmarks")
STAGE_FILES = {
    "raw_parquet": BENCHMARKS_DIR / "stage1_baseline.json",
    "iceberg": BENCHMARKS_DIR / "stage2_iceberg.json",
    "delta": BENCHMARKS_DIR / "stage3_delta.json",
}
STORAGE_FILE = BENCHMARKS_DIR / "storage_sizes.json"


def load_json(path: Path) -> dict | None:
    """Return parsed JSON, or None if the file doesn't exist yet."""
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def fmt_time(stage_data: dict | None, query_key: str) -> str:
    if stage_data is None:
        return "PENDING"
    return f"{stage_data[query_key]['median_seconds']:.2f}s"


def fmt_speedup(baseline_seconds: float | None, current_seconds: float | None) -> str:
    if baseline_seconds is None or current_seconds is None or current_seconds == 0:
        return ""
    return f"({baseline_seconds / current_seconds:.1f}x faster)"


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare query benchmarks across all pipeline stages")
    parser.parse_args()

    stages = {name: load_json(path) for name, path in STAGE_FILES.items()}
    storage = load_json(STORAGE_FILE)

    baseline = stages["raw_parquet"]

    print("\n" + "=" * 42)
    print("Query Performance Comparison")
    print("=" * 42)

    for query_key, query_label in [
        ("query_a", "Query A (full aggregation)"),
        ("query_b", "Query B (date filter)"),
        ("query_c", "Query C (most selective)"),
    ]:
        print(f"\n{query_label}:")
        baseline_seconds = baseline[query_key]["median_seconds"] if baseline else None

        for stage_name, stage_label in [
            ("raw_parquet", "Raw Parquet"),
            ("iceberg", "Iceberg"),
            ("delta", "Delta"),
        ]:
            stage_data = stages[stage_name]
            time_str = fmt_time(stage_data, query_key)
            current_seconds = stage_data[query_key]["median_seconds"] if stage_data else None
            speedup_str = fmt_speedup(baseline_seconds, current_seconds) if stage_name != "raw_parquet" else ""
            print(f"  {stage_label:15s} {time_str:>10s}  {speedup_str}")

    # Headline result
    if stages["raw_parquet"] and stages["iceberg"] and stages["delta"]:
        c_raw = stages["raw_parquet"]["query_c"]["median_seconds"]
        c_iceberg = stages["iceberg"]["query_c"]["median_seconds"]
        c_delta = stages["delta"]["query_c"]["median_seconds"]
        total_speedup = c_raw / c_delta

        print("\n" + "=" * 42)
        print("HEADLINE RESULT")
        print("=" * 42)
        print(f"Query C: {c_raw:.2f}s -> {c_iceberg:.2f}s -> {c_delta:.2f}s")
        print(f"Total speedup: {total_speedup:.1f}x via partitioning + Z-ordering")
    else:
        print("\n(Headline result pending — not all three stages have benchmark data yet)")

    # Storage comparison, if available
    if storage:
        print("\n" + "=" * 42)
        print("Storage Comparison")
        print("=" * 42)
        print(f"Raw CSV (documented):  {storage['raw_csv_documented_gb']:.1f} GB")
        print(f"Raw Parquet:           {storage['raw_parquet']['gb']:.2f} GB "
              f"({storage['raw_parquet']['files']} files)")
        print(f"Iceberg:               {storage['iceberg_partitioned']['gb']:.2f} GB "
              f"({storage['iceberg_partitioned']['files']} files)")
        print(f"Delta (Z-ordered):     {storage['delta_zordered']['gb']:.2f} GB "
              f"({storage['delta_zordered']['files']} files)")
        note = (
            " ** Delta storage grew vs Iceberg — Z-ordering optimizes query "
            "pruning, not storage size; see DECISIONS.md"
            if storage['delta_zordered']['gb'] > storage['iceberg_partitioned']['gb']
            else ""
        )
        if note:
            print(note)

    print(f"\nRun python benchmarks/compare_stages.py after each stage to see cumulative improvement.")


if __name__ == "__main__":
    main()