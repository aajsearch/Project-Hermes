#!/usr/bin/env python3
"""
Filter Bot V2 console.log for EXECUTION stop-loss activity and write context blocks.

For every line that matches an EXECUTION + stop-loss indicator, output a block of:
  - N lines before
  - the matching line
  - N lines after

Output is a single text file with merged (overlapping) context blocks.
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import List, Tuple

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_IN = ROOT / "data" / "console.log"
DEFAULT_OUT = ROOT / "data" / "v2_sl_execution_blocks.txt"


def _find_match_indices(lines: List[str], pattern: re.Pattern[str]) -> List[int]:
    idxs: List[int] = []
    for i, line in enumerate(lines):
        if pattern.search(line):
            idxs.append(i)
    return idxs


def _merge_ranges(ranges: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    if not ranges:
        return []
    ranges.sort(key=lambda r: r[0])
    merged: List[Tuple[int, int]] = [ranges[0]]
    for start, end in ranges[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end + 1:
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))
    return merged


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract EXECUTION stop-loss blocks with context.")
    parser.add_argument("--in", dest="in_path", type=Path, default=DEFAULT_IN, help=f"Input log file (default: {DEFAULT_IN})")
    parser.add_argument("--out", dest="out_path", type=Path, default=DEFAULT_OUT, help=f"Output txt (default: {DEFAULT_OUT})")
    parser.add_argument("--context", type=int, default=10, help="Lines before and after each match (default: 10)")
    args = parser.parse_args()

    if not args.in_path.exists():
        print(f"Input log not found: {args.in_path}")
        return 1

    text = args.in_path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines(keepends=False)

    # Matches executor lines like:
    #  [EXECUTION] stop_loss/market_sell: ...
    #  [EXECUTION] Recorded trade outcome ... is_stop_loss=1 ...
    #  [EXECUTION] Stop-loss skipped: ...
    # Keep it permissive: require [EXECUTION] and one of stop-loss indicators.
    pat = re.compile(r"\[EXECUTION\].*(stop_loss|Stop-loss|is_stop_loss=1|sl_catastrophic|sl_normal_persistence|sl_decay)")

    match_idxs = _find_match_indices(lines, pat)
    if not match_idxs:
        print("No EXECUTION stop-loss matches found.")
        return 2

    n = len(lines)
    ctx = max(0, int(args.context))
    ranges = [(max(0, i - ctx), min(n - 1, i + ctx)) for i in match_idxs]
    merged = _merge_ranges(ranges)

    args.out_path.parent.mkdir(parents=True, exist_ok=True)
    with args.out_path.open("w", encoding="utf-8") as f:
        f.write(f"Source: {args.in_path}\n")
        f.write(f"Matches: {len(match_idxs)} lines\n")
        f.write(f"Merged blocks: {len(merged)}\n")
        f.write(f"Context: +/- {ctx} lines\n")
        f.write("\n" + "=" * 80 + "\n\n")

        for block_i, (start, end) in enumerate(merged, start=1):
            f.write(f"BLOCK {block_i}/{len(merged)}: lines {start+1}-{end+1}\n")
            f.write("-" * 80 + "\n")
            for li in range(start, end + 1):
                f.write(lines[li] + "\n")
            f.write("\n" + "=" * 80 + "\n\n")

    print(f"Wrote {args.out_path} ({len(merged)} blocks from {len(match_idxs)} match lines).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

