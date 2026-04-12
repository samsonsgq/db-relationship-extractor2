# STEP 6 diff summary (SP-only)

Compared files:
- Generated: `fixture_step6/out/relationship_detail.tsv`
- Expected fixture: `src/main/java/com/example/db2lineage/resources/sample_case/RPT_PR_TEST_DEMO_corrected_only.tsv`
- Semantic authority: `relationship_detail_concat_usage_clarified_v2.md`

## High-level counts

- Generated rows: **89**
- Expected rows: **1024**
- Common keys (all columns except `confidence`): **25**
- Confidence-only differences: **23** (`PARSER` expected vs `REGEX` generated)
- Missing from generated (present in expected key-set): **999**
- Extra in generated (absent in expected key-set): **64**

Total remaining differences classified: **1086**.

## Classification of every remaining difference

### 1) implementation still incomplete — **1062 differences**

- **999** expected-key rows are missing in generated output.
  - Dominant missing relationship types: `SELECT_EXPR` (258), `INSERT_SELECT_MAP` (183), `INSERT_TARGET_COL` (152), `WHERE` (112), `VARIABLE_SET_MAP` (89), `SELECT_FIELD` (63), etc.
  - This is primarily missing parser-first coverage for nested procedural SQL statements and assignment propagation in the routine body.
- **63** generated-only rows are extra and reflect fallback-side over/under-modeling or duplication against expected shape.
  - Examples include duplicate `CALL_PARAM_MAP`/`CALL_PROCEDURE` rows emitted from fallback alongside parser rows, and fallback-produced cursor/handler rows whose key shape differs from expected.

### 2) JSqlParser / DB2 syntax limitation — **1 difference**

- **1** generated-only `UNKNOWN` row for `SET ld_end_date =` (`line_no=194`) corresponds to DB2 procedural `CASE` assignment structure not being fully represented in current parser-first nested extraction path.

### 3) MD ambiguity — **0 differences**

- No remaining difference required an MD interpretation change in this pass.

### 4) expected TSV likely stricter or looser than MD — **23 differences**

- The 23 confidence-only deltas (`PARSER` in expected vs `REGEX` in generated) do not violate MD semantics because MD allows both `PARSER` and `REGEX` confidence values when fallback is used truthfully.
- These rows are semantically aligned but fixture is stricter on confidence provenance.

## Parser-first vs fallback used in this step

- **Parser-first retained** for statement parsing and existing extractor paths; no regex-only bypass was introduced.
- **Conservative fallback adjusted** for DB2 procedural fragments where parser coverage is incomplete:
  - tightened fetch-statement detection to avoid label-only false positives,
  - anchored `CURSOR_READ` fetch rows to actual `FETCH` line,
  - anchored control-flow fallback rows to the actual source line,
  - removed statement-level `SET` fallback mapping that was introducing broad mis-anchoring artifacts.
