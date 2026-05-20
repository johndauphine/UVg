#!/usr/bin/env python3
"""Advisory Rust module-size and test-layout report."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path


CFG_TEST_RE = re.compile(r"#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]")
SKIP_DIRS = {".git", "target"}
TEST_ONLY_FILENAMES = {"tests.rs", "testutil.rs"}


@dataclass(frozen=True)
class RustFileReport:
    path: Path
    total_lines: int
    production_lines: int
    inline_test_lines: int
    test_file_lines: int


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Report Rust source line counts and cfg(test) layout. "
            "This is advisory and exits 0 unless --fail-on-warn is set."
        )
    )
    parser.add_argument(
        "root",
        nargs="?",
        default=".",
        type=Path,
        help="repository root to scan (default: current directory)",
    )
    parser.add_argument(
        "--target-lines",
        default=500,
        type=int,
        help="soft production-line target to note (default: 500)",
    )
    parser.add_argument(
        "--limit-lines",
        default=750,
        type=int,
        help="upper advisory production-line threshold (default: 750)",
    )
    parser.add_argument(
        "--inline-test-lines",
        default=250,
        type=int,
        help="inline cfg(test) line threshold to note (default: 250)",
    )
    parser.add_argument(
        "--fail-on-warn",
        action="store_true",
        help="exit 1 when advisory thresholds are exceeded",
    )
    return parser.parse_args(argv)


def iter_rust_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in root.rglob("*.rs"):
        if any(part in SKIP_DIRS for part in path.relative_to(root).parts):
            continue
        files.append(path)
    return sorted(files)


def is_test_file(path: Path) -> bool:
    name = path.name
    return (
        path.parts[0] == "tests"
        or any(part == "tests" or part.endswith("_tests") for part in path.parts[:-1])
        or name.endswith("_tests.rs")
        or name in TEST_ONLY_FILENAMES
    )


def count_inline_test_lines(lines: list[str]) -> int:
    covered: set[int] = set()
    index = 0
    while index < len(lines):
        if index in covered or not CFG_TEST_RE.search(lines[index]):
            index += 1
            continue
        end = cfg_item_end(lines, index)
        covered.update(range(index, end + 1))
        index = end + 1
    return len(covered)


def cfg_item_end(lines: list[str], cfg_index: int) -> int:
    item_index = cfg_index + 1
    while item_index < len(lines):
        stripped = lines[item_index].strip()
        if stripped == "" or stripped.startswith("#["):
            item_index += 1
            continue
        break
    if item_index >= len(lines):
        return cfg_index

    brace_depth = 0
    saw_brace = False
    for index in range(item_index, len(lines)):
        code = strip_line_comment(lines[index])
        brace_depth += code.count("{")
        if "{" in code:
            saw_brace = True
        brace_depth -= code.count("}")
        if saw_brace:
            if brace_depth <= 0:
                return index
        elif ";" in code:
            return index
    return item_index


def strip_line_comment(line: str) -> str:
    split_at = line.find("//")
    if split_at == -1:
        return line
    return line[:split_at]


def analyze_file(path: Path, root: Path) -> RustFileReport:
    rel_path = path.relative_to(root)
    lines = path.read_text(encoding="utf-8").splitlines()
    total = len(lines)
    if is_test_file(rel_path):
        return RustFileReport(rel_path, total, 0, 0, total)

    inline_test_lines = count_inline_test_lines(lines)
    production_lines = max(total - inline_test_lines, 0)
    return RustFileReport(rel_path, total, production_lines, inline_test_lines, 0)


def row_flags(
    row: RustFileReport,
    *,
    target_lines: int,
    limit_lines: int,
    inline_test_lines: int,
) -> list[str]:
    flags: list[str] = []
    if row.production_lines > limit_lines:
        flags.append(f"prod>{limit_lines}")
    elif row.production_lines > target_lines:
        flags.append(f"prod>{target_lines}")
    if row.inline_test_lines > inline_test_lines:
        flags.append(f"inline-test>{inline_test_lines}")
    if row.test_file_lines and row.total_lines > limit_lines:
        flags.append(f"test-file>{limit_lines}")
    return flags


def print_report(rows: list[RustFileReport], args: argparse.Namespace) -> bool:
    rows = sorted(
        rows,
        key=lambda row: (-row.production_lines, -row.total_lines, row.path.as_posix()),
    )
    warning_rows = [
        row
        for row in rows
        if row_flags(
            row,
            target_lines=args.target_lines,
            limit_lines=args.limit_lines,
            inline_test_lines=args.inline_test_lines,
        )
    ]

    print("Rust module size report (advisory)")
    print(
        "Thresholds: "
        f"prod>{args.target_lines} note, "
        f"prod>{args.limit_lines} high, "
        f"inline-test>{args.inline_test_lines} note"
    )
    print("Default exit status is 0; use --fail-on-warn to opt into gating.")
    print()
    print(f"{'total':>6} {'prod':>6} {'inline':>7} {'test':>6}  {'flags':<24} path")
    print(f"{'-' * 6} {'-' * 6} {'-' * 7} {'-' * 6}  {'-' * 24} {'-' * 4}")
    for row in rows:
        flags = row_flags(
            row,
            target_lines=args.target_lines,
            limit_lines=args.limit_lines,
            inline_test_lines=args.inline_test_lines,
        )
        print(
            f"{row.total_lines:6} "
            f"{row.production_lines:6} "
            f"{row.inline_test_lines:7} "
            f"{row.test_file_lines:6}  "
            f"{','.join(flags) or '-':<24} "
            f"{row.path.as_posix()}"
        )

    print()
    print(f"Rust files: {len(rows)}")
    print(f"Files with advisory notes: {len(warning_rows)}")
    print(f"Production lines: {sum(row.production_lines for row in rows)}")
    print(f"Inline cfg(test) lines: {sum(row.inline_test_lines for row in rows)}")
    print(f"Dedicated test-file lines: {sum(row.test_file_lines for row in rows)}")
    return bool(warning_rows)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    root = args.root.resolve()
    if not root.exists():
        print(f"error: root does not exist: {root}", file=sys.stderr)
        return 2
    rows = [analyze_file(path, root) for path in iter_rust_files(root)]
    has_warnings = print_report(rows, args)
    if has_warnings and args.fail_on_warn:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
