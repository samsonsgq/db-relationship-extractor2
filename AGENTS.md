# Agent Instructions for db-relationship-extractor2

## Source of truth
- Always read the latest `relationship_detail.md` first.
- Treat `relationship_detail.md` as the semantic source of truth over generated TSV samples.

## Core extraction constraints
- Keep relationships direct and single-hop only.
- Preserve exact raw line anchoring:
  - `line_no` must be the exact physical source line.
  - `line_content` must be the exact raw source line.
- Prefer truthful weaker rows over fabricated stronger rows.
- Do not fabricate columns, parameter names, or mappings.

## CLI and input model (must stay directory-based)
- Preserve and use these arguments:
  - `--tableDir`
  - `--viewDir`
  - `--functionDir`
  - `--spDir`
  - `--outputDir`
  - `--extraDir` (optional)
- Do **not** redesign CLI into `--sql-path`, `--tables`, `--views`, or similar file-list styles.

## Engineering approach
- Make minimal, incremental changes.
- Keep tests updated with any behavior changes.
- Preserve existing naming and startup flow when possible.
- Do not silently skip files, statement slices, parse failures, or relationships.

## Parsing rule
- Use JSqlParser as the primary parser and AST provider.
- Use narrow fallback logic only for DB2 constructs JSqlParser cannot represent well.
- Do not replace parser-first extraction with regex-only extraction.
