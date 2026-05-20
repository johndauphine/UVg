# Advisory Module Size Report

The project keeps a lightweight local report for Rust module size and test
layout:

```bash
./scripts/module_size_report.py
```

The report is advisory. It prints notes for files above the current production
line guidance, but it exits successfully by default and is not a CI gate. Future
CI use should opt in explicitly with `--fail-on-warn` if the team wants a hard
check.

## Guideline

Treat roughly 500 production lines as the point where a module deserves a quick
readability check, and roughly 750 production lines as a stronger prompt to
look for a cohesive split. These are not mechanical limits. Readability,
cohesion, output fidelity, and straightforward reviews outrank line-count
reductions.

Inline tests are fine when they sit close to small private helpers. When inline
`#[cfg(test)]` blocks grow large or make the production module hard to scan,
prefer sibling `*_tests.rs` files wired through small cfg-only module hooks.

## Columns

- `total`: all lines in the Rust source file.
- `prod`: approximate production lines, excluding detected cfg-only test items
  in production files.
- `inline`: lines covered by immediate `#[cfg(test)]` items in production
  files.
- `test`: lines in dedicated test files such as `*_tests.rs`, `tests.rs`,
  `testutil.rs`, and files under `tests/`.
- `flags`: advisory notes for files over configured thresholds.

The scanner is intentionally simple and local. It is a review aid for spotting
large modules and production/test mixing, not a semantic Rust parser.
