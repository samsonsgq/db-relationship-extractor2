# relationship_detail.tsv — revised semantics and filling matrix (DB2-focused, schema-aware, production-tightened v4.9)

This document is the **semantic contract** for generating and validating `relationship_detail.tsv`.

It is intended to stay aligned with the current sample SQL and TSV, but it is **authoritative over the TSV** when the two diverge. Any mismatch between this document and a generated TSV should be treated as a defect in either:
- the extractor / parser / post-processor, or
- this contract itself.

## Revision scope for v4.9

This revision keeps the v3 tightening and additionally closes several DB2 SQL PL gaps that still allowed two careful implementations to emit different but plausible TSV rows. In particular, v4 strengthens:
- deterministic `line_no` / `line_content` / `line_relation_seq` anchoring
- `SET`, `SELECT ... INTO`, `VALUES ... INTO`, scalar fullselect, and row-value assignment semantics
- cursor result-slot lineage before `FETCH ... INTO`
- handler, diagnostics, and direct DB2 state-token semantics
- sequence usage via `NEXT VALUE FOR`
- default mapping policy and precedence rules for overlapping relationship families
- mandatory stronger-row expansion for fully literal dynamic SQL
- dynamic USING-expression usage completion
- tighter outermost-only `FUNCTION_EXPR_MAP` emission for nested wrappers
- clearer scalar `RETURN` expression boundary rules
- canonical handler-condition token forms
- clarified `FOR` loop treatment and cross-line slot-order rules
- stronger validator defects for avoidable duplicate/over-emitted semantics
- cursor operation-class normalization on `CURSOR_READ.target_field`
- scalar `RETURN expr` narrowed to operand-level `RETURN_VALUE` plus callable parameter lineage only
- cross-line slot-misalignment validator defect for recoverable logical slot order
- declared conditions, `SIGNAL`, and `RESIGNAL` semantics
- cursor operation semantics for `OPEN`, `FETCH`, `CLOSE`, implicit cursor iteration, and rowset-aware placeholders
- dynamic SQL semantics for `PREPARE`, `EXECUTE`, `EXECUTE IMMEDIATE`, `OPEN ... FOR`, and `USING` parameter bindings
- procedural control-structure clarification for searched `CASE` statements, labels, `LEAVE`, and `ITERATE`
- generated/default-value policy and broader DB2 special-register normalization
- source-side alias/object disambiguation for field-level rows when plain field tokens would be ambiguous
- recovered dynamic-SQL logical ordering projected onto the runtime statement line
- reserved-status clarification for `ROWSET` and `RESULT_SET` until dedicated relationship families exist
- explicit `FOR` loop normalization rules for cursor-backed and select-backed iteration
- validator defects for source-side alias loss and non-canonical recovered-dynamic ordering
- family-level source-side alias preservation requirements for `MERGE_*`, `UPDATE_SET_MAP`, `INSERT_SELECT_MAP`, and `VARIABLE_SET_MAP`
- a more mechanical recovered dynamic-SQL ordering recipe for validator-friendly implementation
- explicit reserved-only status alignment for `ROWSET` and `RESULT_SET` in the object-type enumeration

## Core principle

Each row records **one direct, single-hop relationship** found in a source SQL object.

The core meaning of a row must be readable from these main columns:
- `source_object_type`
- `source_object`
- `source_field`
- `target_object_type`
- `target_object`
- `target_field`
- `relationship`
- `line_no`
- `line_relation_seq`
- `line_content`

## General extraction rules

### Directness
Only emit **direct single-hop** relationships.
Do **not** collapse multi-hop propagation into one row.

Examples:
- `A -> V1` and `V1 -> T1` are two direct rows, not one inferred `A -> T1` row.
- `FUNCTION_EXPR_MAP` captures that a function result is used in an assignment expression; it does not replace argument lineage into the function itself.

### Additional directness clarifications
The phrase **direct** must be interpreted textually and slot-locally, not by downstream business meaning.

Rules:
- For nested functions such as `COALESCE(F1(X), F2(Y), 0)`, operand tokens `X`, `Y`, and `CONSTANT:0` are direct usage-side participants in the outer expression, while `FUNCTION_EXPR_MAP` models only the direct outermost function-result-to-target-slot dependency for `F1` and `F2` when those function results themselves are the immediate contributors to the written target slot.
- When a function call directly contributes value to a target slot, the contract must follow one uniform policy. Under the default profile in this document, emit `FUNCTION_EXPR_MAP` for every named outermost function call, including DB2 built-in scalar functions and user-defined functions, when that function result directly contributes to the written target slot. Do not leave this to implementation preference.
- Do not emit cascading target-slot `FUNCTION_EXPR_MAP` rows for every nested wrapper in a stack such as `CHAR(DECIMAL(ABS(v_n),10,2))`. Only the outermost function result that directly lands in the target slot may emit `FUNCTION_EXPR_MAP`; inner function calls remain visible through usage-side rows and parameter lineage unless another explicit rule requires more.
- For correlated subqueries, an outer-scope token referenced inside the subquery is direct only for the subquery predicate or expression role where it textually appears. It must not bypass the subquery boundary to become a direct target-flow row unless another explicit contract rule requires that.
- For `CALL` `OUT` / `INOUT` receive semantics, the direct receive event is the formal output parameter into the local receiving variable; do not bypass the callable boundary and map deeper procedure internals directly into the local variable.
- For cursor fetches, do not collapse base-table or base-expression inputs directly into fetched variables when named cursor result slots exist. Model cursor result-slot lineage first, then `CURSOR_FETCH_MAP`.
- For row-value assignment and row-value `UPDATE`, directness is evaluated per aligned slot, not per statement as a whole.

### Exact source fidelity
- `line_no` must be the **exact physical source line number** from the SQL file.
- `line_content` must be the **exact original raw source line** from the SQL file.
- If a row's semantics are correct but the `line_no` or `line_content` is wrong, the row is still considered defective.

### Deterministic statement anchoring

To make `line_no`, `line_content`, and `line_relation_seq` stable across implementations, every emitted row must follow this anchoring rule:

1. **Usage-side rows**
   - Anchor to the exact physical line where the direct participating token appears in the raw SQL text.
   - This applies to relationships such as:
     - `SELECT_FIELD`
     - `SELECT_EXPR`
     - `UPDATE_SET`
     - `WHERE`
     - `JOIN_ON`
     - `MERGE_MATCH`
     - `GROUP_BY`
     - `ORDER_BY`
     - `HAVING`
     - `CONTROL_FLOW_CONDITION`
     - operand-form `RETURN_VALUE`

2. **Target-flow / slot-definition rows**
   - Anchor to the physical line that declares the receiving target slot or direct statement slot.
   - This applies to relationships such as:
     - `INSERT_TARGET_COL`
     - `UPDATE_TARGET_COL`
     - `MERGE_TARGET_COL`
     - `CREATE_VIEW_MAP`
     - `INSERT_SELECT_MAP`
     - `UPDATE_SET_MAP`
     - `MERGE_SET_MAP`
     - `MERGE_INSERT_MAP`
     - `VARIABLE_SET_MAP`
     - `CURSOR_SELECT_MAP`
     - `CURSOR_FETCH_MAP`
     - `FUNCTION_PARAM_MAP`
     - `CALL_PARAM_MAP`
     - `TABLE_FUNCTION_RETURN_MAP`
     - `SPECIAL_REGISTER_MAP`
     - `DIAGNOSTICS_FETCH_MAP`
     - `SEQUENCE_VALUE_MAP`
     - `FUNCTION_EXPR_MAP`

3. **Anchor examples**
   - `SET v_msg = 'A' || v_x;`
     - usage rows for `'A'` and `v_x` anchor to the token line
     - mapping rows into `v_msg` anchor to the line containing `SET v_msg =`
   - `SELECT c1, COALESCE(c2, '') INTO v1, v2 FROM T;`
     - `SELECT_FIELD` / `SELECT_EXPR` rows anchor to the select-item token lines
     - `VARIABLE_SET_MAP` rows anchor to the line where each `INTO` target appears
   - `UPDATE T SET (C1, C2) = (SELECT X, Y FROM S ... )`
     - `UPDATE_SET` usage rows anchor to the source-token lines inside the scalar fullselect
     - `UPDATE_SET_MAP` rows anchor to the physical line where `C1` or `C2` appears in the target slot list
   - `FETCH c1 INTO v1, v2;`
     - `CURSOR_FETCH_MAP` rows anchor to the line where `v1`, `v2` appear in the `INTO` target list

4. **Forbidden ambiguity**
   - Do **not** choose source-token line anchoring for target-flow rows.
   - Do **not** choose target-slot anchoring for usage-side rows.
   - Do **not** anchor a row to an arbitrary statement-first line when a more specific token/slot line exists.

### Schema-aware expansion
The extractor may use schema/catalog metadata (DDL or live database metadata) to resolve:
- object types
- column ownership
- `SELECT *` expansion
- named function/procedure parameters
- target-column alignment for `INSERT ... SELECT` and `MERGE`

If required schema metadata is unavailable:
- prefer a weaker but truthful row set,
- or emit lower-confidence fallback rows,
- but do **not** invent columns or parameter names.

## Literal, special register, and system-value rule

Ensure field-level lineage captures literals and DB2 system-derived values wherever they directly participate in SQL semantics.

### Canonical token form
Use:

- `CONSTANT:<VALUE>` for literals and DB2 special registers/system constants  
  Examples:
  - `CONSTANT:'ACTIVE'`
  - `CONSTANT:1`
  - `CONSTANT:NULL`
  - `CONSTANT:CURRENT TIMESTAMP`
  - `CONSTANT:CURRENT DATE`
  - `CONSTANT:CURRENT TIME`
  - `CONSTANT:CURRENT SCHEMA`
  - `CONSTANT:CURRENT PATH`
  - `CONSTANT:CURRENT SERVER`
  - `CONSTANT:USER`
  - `CONSTANT:SESSION_USER`

Normalize DB2 special-register text to its canonical uppercase spelling in `source_field`.

### Sequence token form

Use:

- `SEQUENCE:<schema.sequence_name>` for sequence references used through `NEXT VALUE FOR`

Examples:
- `SEQUENCE:RPT.GLOBAL_SEQ_ACCOUNT_EVENT_UNIQUE_ID`

Do **not** encode a sequence reference as `CONSTANT:<VALUE>`.

### Where literal/system tokens are allowed
Literal/system tokens may appear in `source_field` for:
- `SELECT_EXPR`
- `INSERT_SELECT_MAP`
- `UPDATE_SET`
- `UPDATE_SET_MAP`
- `MERGE_SET_MAP`
- `MERGE_INSERT_MAP`
- `VARIABLE_SET_MAP`
- `FUNCTION_PARAM_MAP`
- `CALL_PARAM_MAP`
- `SPECIAL_REGISTER_MAP`
- `DIAGNOSTICS_FETCH_MAP`
- `RETURN_VALUE`
- `WHERE`
- `JOIN_ON`
- `HAVING`
- `CONTROL_FLOW_CONDITION`

### Relationship choice precedence
- Use `SPECIAL_REGISTER_MAP` for DB2 special-register assignment/mapping into a target column or variable when that is the most specific meaning.
- Use `DIAGNOSTICS_FETCH_MAP` for diagnostic-state fetches, including direct `SQLCODE` / `SQLSTATE` assignment.
- Use `SEQUENCE_VALUE_MAP` for `NEXT VALUE FOR` sequence usage that directly lands in a target slot.
- Otherwise use the context-appropriate generic relationship (`INSERT_SELECT_MAP`, `UPDATE_SET_MAP`, `SELECT_EXPR`, `RETURN_VALUE`, etc.).

## Column semantics

### `source_object_type`
Type of the SQL source object that owns the row.

Common values:
- `VIEW_DDL`
- `FUNCTION`
- `PROCEDURE`
- `SCRIPT`

### `source_object`
Owning SQL object name.

Examples:
- `INTERFACE.V_API_MTM_REVAL`
- `INTERFACE.FN_GET_CODE_MAP_VALUE`
- `INTERFACE.PI_API_MTM_REVAL_DEMO`
- `EXTRA_PATTERNS`

### `source_field`
Primary source-side token for field-level relationships.

Rules:
- Field-level rows: populate with the direct source or participating token.
- Object-level rows: leave empty.
- If the source is a literal or DB2 special register/system value, use `CONSTANT:<VALUE>`.
- If the source is a sequence reference used through `NEXT VALUE FOR`, use `SEQUENCE:<schema.sequence_name>`.
- For callable-parameter mappings:
  - `FUNCTION_PARAM_MAP` / `CALL_PARAM_MAP` use the actual argument token in `source_field`.
- For `FUNCTION_EXPR_MAP`, use the called function name in `source_field`.

### `target_object_type`
Resolved object type on the right side of the direct relationship.

Common values:
- `TABLE`
- `VIEW`
- `SESSION_TABLE`
- `CTE`
- `DERIVED_TABLE`
- `FUNCTION`
- `PROCEDURE`
- `CURSOR`
- `VARIABLE`
- `CONDITION`
- `STMT_HANDLE`
- `RESULT_SET` *(reserved-only until dedicated relationship families are introduced; do not emit merely to bypass required `CURSOR_*` normalization)*
- `ROWSET` *(reserved-only until dedicated relationship families are introduced; do not emit merely to bypass required `CURSOR_*` normalization)*
- `UNKNOWN`

### `target_object`
Resolved object on the right side of the direct relationship.

Rules:
- Use the real resolved object whenever possible.
- For unresolved placeholders, use a stable placeholder that matches the unresolved class:
  - `UNKNOWN_DYNAMIC_SQL` for unresolved dynamic SQL text
  - `UNKNOWN_UNSUPPORTED_STATEMENT` for unsupported but recognized statements such as utilities/admin commands
  - `UNKNOWN_UNRESOLVED_OBJECT` for syntactically parseable cases whose real object cannot be resolved

### `target_field`
Resolved field / slot on the right side of the direct relationship.

Rules:
- Field-level rows: populate with the concrete target / participating field.
- Object-level rows: leave empty.
- For assignment-slot relationships (`UPDATE_SET`, `UPDATE_SET_MAP`, etc.), this is the target column or variable receiving the value.
- For parameter-mapping relationships, use the **formal parameter name** when known; otherwise use a positional slot like `$1`, `$2`, ...
- For handler rows, use `EXIT` or `CONTINUE` when that handler kind is explicitly declared.
- For dynamic-SQL rows, `target_field` may store a direct operation class such as `PREPARE`, `EXECUTE`, `EXECUTE_IMMEDIATE`, or `OPEN_CURSOR` when recoverable from the SQL text.

### `relationship`
One of the allowed relationship types defined below.

### `line_no`
Exact physical source line number.

### `line_relation_seq`
Stable per-line sequence number within the same source object.

Rules:
- Partition by (`source_object_type`, `source_object`, `line_no`)
- Number from `0`
- If a source line produces only one relationship row, use `0`
- This is a **line-level** sequence only. It does **not** number the whole SQL statement across multiple lines.
- The sequence must follow the **natural SQL relationship order on that line**, not a generic dictionary sort.

Natural SQL ordering rules:
1. Keep original left-to-right SQL appearance order whenever the line itself provides clear positional order.
2. For ordered slot relationships, preserve statement slot order:
   - `INSERT_TARGET_COL`: target-column declaration order inside `INSERT (...)`
   - `CREATE_VIEW_MAP`, `INSERT_SELECT_MAP`, `TABLE_FUNCTION_RETURN_MAP`, `CURSOR_SELECT_MAP`: select-item / mapping slot order
   - `UPDATE_TARGET_COL`: `SET` target assignment order
   - `UPDATE_SET_MAP`: target assignment order, then source-expression appearance order if more than one direct source participates
   - `MERGE_TARGET_COL`, `MERGE_SET_MAP`, `MERGE_INSERT_MAP`: branch-local target / mapping slot order
   - `FUNCTION_PARAM_MAP`, `CALL_PARAM_MAP`: callable argument order
   - `VARIABLE_SET_MAP`, `SPECIAL_REGISTER_MAP`, `DIAGNOSTICS_FETCH_MAP`, `SEQUENCE_VALUE_MAP`, `FUNCTION_EXPR_MAP`: target assignment / slot order
   - `CURSOR_FETCH_MAP`: fetch target order
3. For direct field-usage and predicate rows, preserve token appearance order on the source line:
   - `SELECT_FIELD`, `SELECT_EXPR`, `WHERE`, `JOIN_ON`, `GROUP_BY`, `HAVING`, `ORDER_BY`, `MERGE_MATCH`, `UPDATE_SET`, `CONTROL_FLOW_CONDITION`, `RETURN_VALUE`
4. For object-level rows, preserve statement encounter order on that line:
   - `SELECT_TABLE`, `SELECT_VIEW`, `INSERT_TABLE`, `UPDATE_TABLE`, `DELETE_TABLE`, `DELETE_VIEW`, `TRUNCATE_TABLE`, `MERGE_INTO`, `CALL_FUNCTION`, `CALL_PROCEDURE`, `CTE_DEFINE`, `CTE_READ`, `UNION_INPUT`, `CREATE_VIEW`, `CREATE_TABLE`, `CREATE_PROCEDURE`, `CREATE_FUNCTION`, `UNKNOWN`, `CURSOR_DEFINE`, `CURSOR_READ`, `DYNAMIC_SQL_EXEC`, `EXCEPTION_HANDLER_MAP`
5. When one physical source line mixes multiple relationship families, apply this bucket order before the family-local natural order: object-level rows first, then usage-side rows, then target-flow / slot-definition rows.
6. Only use a deterministic fallback sort when the parser cannot recover meaningful natural order. In that fallback case, sort by:
   `relationship`, `target_object_type`, `target_object`, `target_field`, `source_field`, `line_content`

### `line_content`
Exact original raw source line from the SQL file.

### `confidence`
Extraction confidence.

Allowed values:
- `PARSER`
- `REGEX`
- `DYNAMIC_LOW_CONFIDENCE`

## Stable output ordering

For stable TSV generation, rows should be ordered primarily by:
1. source-object traversal order
2. source line order
3. `line_relation_seq`

When multiple rows come from the same (`source_object_type`, `source_object`, `line_no`) group, use the natural SQL ordering described above.

## `SELECT *` expansion rule

When SQL uses `SELECT *`, the extractor may expand `*` into concrete columns **only if**:
- source schema metadata is available,
- source column order is stable and known,
- and the downstream mapping target can be aligned truthfully.

If those conditions are not met:
- emit only object-level read rows,
- or emit lower-confidence fallback rows,
- but do **not** fabricate field-level rows.

## Callable parameter naming rule

For both procedures and functions:
- use the **formal parameter name** in `target_field` when callable signature metadata is available;
- otherwise use positional slots `$1`, `$2`, ...;
- do not mix named and positional styles for the same callable when the formal signature is known.

## Default mapping profile

Unless an alternate named extraction profile is explicitly enabled, the extractor must use this **default mapping profile**.

### Usage-side default
Usage-side relationships must include all direct participating tokens required by this contract for the active SQL context, including when applicable:

- columns
- variables
- routine parameters
- literals / constants
- special registers
- direct sequence references
- direct function-call usage tokens
- direct diagnostic-state tokens
- direct null-check tokens such as `CONSTANT:NULL`

### Mapping-side default
Mapping-side relationships must include the direct value contributors that land in the written target slot.

Rules:
- emit mapping rows for tokens that directly contribute to the final written value
- do **not** emit mapping rows for tokens that are only control or predicate inputs and do not themselves contribute value
- if one token is both a control token and a value contributor in the same direct expression role, it may legitimately appear in both usage-side and mapping-side rows

Examples:
- `COALESCE(X, Y, 0)` written into a target:
  - usage rows include `X`, `Y`, `CONSTANT:0`
  - mapping rows include `X`, `Y`, `CONSTANT:0`
- `CASE WHEN FLAG = 'Y' THEN AMT ELSE 0 END` written into a target:
  - usage rows include `FLAG`, `CONSTANT:'Y'`, `AMT`, `CONSTANT:0`
  - mapping rows include `AMT`, `CONSTANT:0`
  - `FLAG` and `CONSTANT:'Y'` are control tokens only and must not produce `*_MAP` rows under the default profile

### Duplicate policy
A usage-side row and a mapping-side row for the same token are **not** duplicates when they serve different semantic roles.
Rows are duplicates only when they express the same direct semantic role under the same relationship family.

## Relationship precedence rules

When multiple relationship families could plausibly describe the same direct semantic fact, apply the most specific valid relationship and follow these precedence rules.

1. **`SELECT_FIELD` vs `SELECT_EXPR`**
   - Use `SELECT_FIELD` only when the select item is a naked resolved column reference with no wrapper, cast, function, operator, literal, special register, sequence reference, or subquery.
   - Otherwise use `SELECT_EXPR`.
   - Do not emit both for the same direct select-item token in the same semantic role.

2. **`SELECT_EXPR` vs `VARIABLE_SET_MAP`**
   - For variable-target assignment statements, `VARIABLE_SET_MAP` is the mandatory target-flow relationship.
   - `SELECT_EXPR` may appear only as a usage-side companion row for complex right-hand-side expressions.
   - For a plain single-token assignment, emit `VARIABLE_SET_MAP` only by default.

3. **`UPDATE_SET` vs `UPDATE_SET_MAP`**
   - `UPDATE_SET` is usage-side only.
   - `UPDATE_SET_MAP` is target-flow only.
   - Both may coexist for the same direct token when one explains expression participation and the other explains value flow into the written target.

4. **`MERGE_MATCH` vs `WHERE` / `JOIN_ON`**
   - Tokens inside `MERGE ... ON ...` must use `MERGE_MATCH`.
   - Do not relabel `MERGE ... ON ...` tokens as `WHERE` or `JOIN_ON`.

5. **`SPECIAL_REGISTER_MAP` vs generic `*_MAP`**
   - When a DB2 special register directly lands in a concrete target slot, emit `SPECIAL_REGISTER_MAP`.
   - Context-appropriate usage rows may still use `SELECT_EXPR`, `UPDATE_SET`, `MERGE_MATCH`, or other usage-side relationships.

6. **`DIAGNOSTICS_FETCH_MAP` vs `VARIABLE_SET_MAP`**
   - Any direct read of DB2 diagnostic state such as `SQLCODE`, `SQLSTATE`, `ROW_COUNT`, `MESSAGE_TEXT`, and similar `GET DIAGNOSTICS` items must use `DIAGNOSTICS_FETCH_MAP`.
   - Do not use `VARIABLE_SET_MAP` for the same direct diagnostic-state read.

7. **`FUNCTION_EXPR_MAP` vs `FUNCTION_PARAM_MAP`**
   - `FUNCTION_PARAM_MAP` models actual-argument-to-formal-parameter lineage only.
   - `FUNCTION_EXPR_MAP` models function-result-to-target-slot lineage only.

8. **`CALL_PARAM_MAP` vs `VARIABLE_SET_MAP`**
   - Procedure parameter passing and `OUT` / `INOUT` receive semantics must use `CALL_PARAM_MAP`.
   - Do not emit `VARIABLE_SET_MAP` for the same direct procedure-parameter receive event.

9. **Usage rows vs mapping rows**
   - Usage rows and mapping rows serve different purposes and must not be deduplicated across relationship families.

10. **Cursor-source lineage vs fetched-variable lineage**
    - When a named cursor exists, do not collapse base-table source tokens directly into fetched variables.
    - First emit cursor result-slot lineage, then emit `CURSOR_FETCH_MAP` from cursor slot to receiving variable or record slot.

11. **`SELECT ... INTO` / `VALUES ... INTO`**
    - Under this contract, the target-flow relationship for `SELECT ... INTO` and `VALUES ... INTO` is `VARIABLE_SET_MAP`.
    - Do not invent a separate relationship family unless this contract is explicitly revised again.

12. **Sequence tokens**
    - `NEXT VALUE FOR <seq>` must never be represented as `CONSTANT:<VALUE>`.
    - Use `SEQUENCE:<schema.sequence_name>` and `SEQUENCE_VALUE_MAP` when the sequence value directly lands in a target slot.

13. **`FUNCTION_EXPR_MAP` vs operand-only mapping rows**
    - When a named function call contributes a direct value to a target slot, emit `FUNCTION_EXPR_MAP` exactly once for the outermost called function result that directly lands in the target slot.
    - Under the default profile, this applies to both DB2 built-in scalar functions and user-defined functions.
    - Do not emit a stack of target-slot `FUNCTION_EXPR_MAP` rows for every nested wrapper unless this contract later defines a stronger specialized rule.
    - This row does not replace operand-level usage rows or `FUNCTION_PARAM_MAP` rows for the function arguments.

13.a **`CALL_PARAM_MAP` vs usage-side expression rows for complex actual arguments**
    - `CALL_PARAM_MAP` models only the actual-to-formal transfer or the direct `OUT` / `INOUT` receive event.
    - When an actual `CALL` argument is a complex expression, emit usage-side rows for every direct participating token inside that argument using the contract's scalar-expression usage relationship rules.
    - Do not treat `CALL_PARAM_MAP` as a substitute for required usage-side rows inside a complex actual argument.

14. **Special registers / sequences vs generic branch-local mapping**
    - When a DB2 special register or sequence directly lands in a target slot, emit only `SPECIAL_REGISTER_MAP` or `SEQUENCE_VALUE_MAP` for that direct mapping fact.
    - Do not additionally emit a generic `INSERT_SELECT_MAP`, `UPDATE_SET_MAP`, `MERGE_SET_MAP`, or `MERGE_INSERT_MAP` row for the same direct mapping fact.

15. **Procedural `CASE` statement vs value-producing `CASE` expression**
    - Use `CONTROL_FLOW_CONDITION` for procedural `CASE` statement branch conditions.
    - Use value-expression usage relationships only for `CASE` expressions that produce values inside `SELECT`, `SET`, `VALUES`, `INSERT`, `UPDATE`, `MERGE`, `CALL`, or `RETURN`.

16. **Handler declarations**
    - Handler condition declarations must use `EXCEPTION_HANDLER_MAP`, never `CONTROL_FLOW_CONDITION`.
    - Regardless of nested block scope, `EXCEPTION_HANDLER_MAP.target_object` must be the owning routine name, not a block label or implementation-specific local-scope placeholder.

17. **Dynamic SQL vs generic `UNKNOWN`**
    - Prefer `DYNAMIC_SQL_EXEC` for explicit dynamic-SQL flow.
    - Use `UNKNOWN` only for unsupported or unresolved non-dynamic statements that are outside every stronger relationship family defined by this contract.

## Variable-target assignment rule

The following statement families all use `VARIABLE_SET_MAP` as the mandatory target-flow relationship:

- `SET v = expr`
- `VALUES expr INTO v`
- `VALUES (expr1, expr2, ...) INTO (v1, v2, ...)`
- `SELECT item1, item2, ... INTO v1, v2, ...`
- scalar fullselect assignment to a variable
- row-value assignment to variables

### Slot matching
- preserve target-slot order exactly as written in the SQL text
- preserve source-slot order exactly as written in the SQL text
- match source and target positionally unless DB2 semantics explicitly define a different alignment
- if counts do not match and the SQL is not valid DB2, do not fabricate rows; emit truthful fallback rows or validation defects instead

### Usage vs mapping behavior
- For a plain single-token source, emit `VARIABLE_SET_MAP` only by default.
- For a complex expression source, emit:
  - `VARIABLE_SET_MAP` for direct value contributors into the receiving variable
  - `SELECT_EXPR` usage rows for direct participating right-hand-side tokens

### `SELECT ... INTO`
- use `SELECT_FIELD` for a naked column select item
- use `SELECT_EXPR` for a computed select item
- use `VARIABLE_SET_MAP` for the target-flow row into each receiving variable
- do not emit both `SELECT_FIELD` and `SELECT_EXPR` for the same direct select-item token in the same semantic role

### `VALUES ... INTO`
- use `VARIABLE_SET_MAP` for target-flow rows
- use `SELECT_EXPR` usage rows for direct participating expression tokens when the source expression is complex

### Scalar fullselect assignment
For assignments such as:

```sql
SET v_x =
(
    SELECT MAX(T.COL1)
      FROM T
     WHERE T.ID = p_id
);
```

emit:
- object read rows for the fullselect source objects
- usage rows for direct fullselect expression and predicate tokens
- `VARIABLE_SET_MAP` rows for direct value contributors into `v_x`

## 6.x Complex Expression Usage Completion Rules

To improve consistency, explainability, and validator determinism, the extractor must emit additional **usage-side** rows for complex expressions, even when the final value-source mapping is already captured by a `_MAP` relationship.

These rules are **normative** and work together with the default mapping profile above.  
They add mandatory usage-side visibility without weakening the required target-flow mapping behavior.

---

### 6.x.1 Rule A — `CASE WHEN` condition tokens must emit usage rows

For a `CASE WHEN ... THEN ... ELSE ... END` expression:

- Every **direct participating token** inside each `WHEN` condition **must** emit a usage-side row.
- Required usage-side relationship:
  - `SELECT_EXPR` when the `CASE` appears inside a `SELECT` projection
  - `SELECT_EXPR` when the `CASE` appears inside a scalar variable-assignment expression
  - `UPDATE_SET` when the `CASE` appears inside an `UPDATE SET` assignment expression

Direct participating tokens include:

- source columns
- local variables
- routine parameters
- literals / constants
- special registers
- direct function calls used in the condition

#### Example

```sql
CASE
    WHEN DEAL.TRADE_DATE = ld_biz_date AND DEAL.BUY_SELL_FLAG = 'B'
         THEN 'OTC_STO_PURCHASE_TRADE_DATE'
```

Expected usage-side rows include:

- `DEAL.TRADE_DATE`
- `ld_biz_date`
- `DEAL.BUY_SELL_FLAG`
- `CONSTANT:'B'`

These rows are **usage rows only**.  
They do **not** replace the final `_MAP` row from the selected branch result into the target column.

---

### 6.x.2 Rule B — `CASE` result literals must emit usage rows

If a `THEN` or `ELSE` branch returns a literal or constant value, that returned value must emit:

- one **usage-side** row
- plus the existing final `_MAP` row into the target column or variable

#### Example

```sql
THEN 'OTC_STO_PURCHASE_TRADE_DATE'
```

Expected rows:

- `SELECT_EXPR` for `CONSTANT:'OTC_STO_PURCHASE_TRADE_DATE'`
- `INSERT_SELECT_MAP` / `UPDATE_SET_MAP` / `MERGE_SET_MAP` / `MERGE_INSERT_MAP` / `VARIABLE_SET_MAP`, as appropriate for the actual assignment target

#### Rationale

This keeps complex `CASE` blocks consistent with simpler projection logic where projected literals are already visible as usage rows.

---

### 6.x.3 Rule C — `NULL` is a participating token in `IS NULL` / `IS NOT NULL`

For predicates or expression branches that explicitly use:

- `IS NULL`
- `IS NOT NULL`
- `= NULL`
- `<> NULL`
- or similar direct null checks

the extractor must treat `NULL` as a direct participating token.

Expected usage representation:

- `source_field = CONSTANT:NULL`

Relationship:

- use the same usage-side relationship as the surrounding expression context
  - typically `WHERE`
  - or `SELECT_EXPR` if the null check appears inside a projected expression such as `CASE`

#### Example 1

```sql
AND DEAL.REVERSE_TS IS NULL
```

Expected rows include:

- field-side token row for `DEAL.REVERSE_TS`
- literal-side token row for `CONSTANT:NULL`

#### Example 2

```sql
WHEN POSN.CLOSE_OUT_AMOUNT IS NOT NULL THEN POSN.CLOSE_OUT_AMOUNT
```

Expected rows include:

- `POSN.CLOSE_OUT_AMOUNT`
- `CONSTANT:NULL`

---

### 6.x.4 Rule D — Usage rows are mandatory for direct tokens; mapping rows remain concise

To avoid ambiguity:

- **Usage-side rows** must include all direct participating tokens for the cases covered by this section.
- **Mapping rows** should remain concise and only describe the final direct value contribution into the target.

This means it is valid and expected to have both:

- `SELECT_EXPR` rows for condition tokens and branch-result literals
- one final `_MAP` row for the chosen result value into the target

#### Example

For:

```sql
CASE
    WHEN DEAL.TRADE_DATE = ld_biz_date AND DEAL.BUY_SELL_FLAG = 'B'
         THEN 'OTC_STO_PURCHASE_TRADE_DATE'
```

Expected:

- usage rows for:
  - `DEAL.TRADE_DATE`
  - `ld_biz_date`
  - `DEAL.BUY_SELL_FLAG`
  - `CONSTANT:'B'`
  - `CONSTANT:'OTC_STO_PURCHASE_TRADE_DATE'`
- one final mapping row from:
  - `CONSTANT:'OTC_STO_PURCHASE_TRADE_DATE'`
  - into the target column

These rows serve different purposes and must not be treated as duplicates.

---

### 6.x.5 Rule E — No AST-internal synthetic nodes

These usage-completion rules must **not** be interpreted as permission to emit arbitrary parser-internal nodes.

Do **not** emit:

- abstract AST node labels
- synthetic parser-only placeholders
- internal temporary expression nodes that do not have direct SQL text representation

Only emit tokens that are directly visible in the SQL text, such as:

- columns
- variables
- parameters
- constants
- special registers
- named functions
- direct sequence references

---

## 6.x Validator Clarification for Complex Expressions

When validating `CASE`, `COALESCE`, `IS NULL`, and `IS NOT NULL` constructs:

- absence of required usage-side rows for direct participating tokens is a validation defect
- absence of parser-internal synthetic nodes is **not** a defect
- final `_MAP` rows and usage-side rows serve different purposes and must not be treated as duplicates
- validators must distinguish between:
  - **usage completeness**
  - **mapping conciseness**

Both are required, but they apply to different row types.

### 6.x.6 String Concatenation (`||`) Rules

To ensure complete and consistent lineage for DB2 string construction, the extractor must explicitly handle the SQL concatenation operator `||`.

These rules apply when `||` appears in:

- `SELECT` projections
- `INSERT ... SELECT`
- `UPDATE ... SET`
- `MERGE`
- `SET` assignments to variables
- procedure / function call argument construction
- any other expression that constructs a final string value

#### Rule A — All direct participating tokens in `||` must emit usage rows

For an expression such as:

```sql
'a' || FIELD1
```

or

```sql
'prefix=' || CHAR(v_count) || ', region=' || COALESCE(v_region, '')
```

every direct participating token in the concatenation chain must emit a usage-side row.

Direct participating tokens include:

- literals / constants
- columns
- variables
- parameters
- special registers
- direct function-call results used in the concatenation
- fallback literals inside `COALESCE`

Required usage relationship:

- `SELECT_EXPR` when the concatenation appears inside a `SELECT`
- `SELECT_EXPR` when the concatenation appears inside a scalar variable-assignment expression
- `UPDATE_SET` when the concatenation appears inside an `UPDATE SET` assignment expression
- the analogous usage-side relationship for other target-writing contexts

#### Rule B — Final target mapping must still be emitted

Usage rows do not replace final propagation mapping rows.

If the concatenation result is written into a target column or variable, also emit the appropriate final mapping rows:

- `INSERT_SELECT_MAP` for `INSERT ... SELECT`
- `UPDATE_SET_MAP` for `UPDATE`
- `MERGE_SET_MAP` / `MERGE_INSERT_MAP` for `MERGE`
- `VARIABLE_SET_MAP` for variable assignment

This means a concatenation expression normally produces both:

- usage-side rows for all direct participating tokens
- mapping-side rows for all direct value contributors into the final target

#### Rule B.1 — Concatenation inside `INSERT ... SELECT` requires both usage rows and mapping rows

For an expression inside `INSERT ... SELECT` that uses concatenation, such as:

```sql
INSERT INTO A (COL1)
SELECT 'a' || FIELD1
FROM B;
```

the extractor must emit both:

- usage-side rows for all direct participating tokens in the select expression
- mapping-side rows for all direct value contributors into the insert target column

Expected minimum result:

- `SELECT_EXPR` for `CONSTANT:'a'`
- `SELECT_EXPR` for `FIELD1`
- `INSERT_SELECT_MAP` for `CONSTANT:'a' -> A.COL1`
- `INSERT_SELECT_MAP` for `FIELD1 -> A.COL1`

These rows serve different purposes and must not be treated as duplicates.

#### Rule B.2 — Concatenation inside variable assignment requires both usage rows and direct assignment mapping

For variable assignment using concatenation, such as:

```sql
SET v_msg = 'cnt=' || CHAR(v_count);
```

the extractor must emit both:

- usage-side rows for all direct participating tokens in the concatenation expression
- direct assignment mappings into the variable target

Expected minimum result:

Usage-side rows:

- `SELECT_EXPR` for `CONSTANT:'cnt='`
- `SELECT_EXPR` for `v_count`

Mapping-side rows:

- `VARIABLE_SET_MAP` for `CONSTANT:'cnt=' -> v_msg`
- `VARIABLE_SET_MAP` for `v_count -> v_msg`

These rows serve different purposes and must not be treated as duplicates.

Usage rows do not replace the required final assignment mappings, and final assignment mappings do not satisfy the usage-row requirement.

#### Additional validator clarification

For `||` expressions:

- emitting only `SELECT_EXPR` without the final target mapping is a defect
- emitting only the final target mapping without required usage rows is a defect
- for `SET var = a || b`, both usage-side rows and `VARIABLE_SET_MAP` rows are required
- emitting both is correct and expected

#### Rule C — Do not collapse concatenation into a synthetic internal node

Do **not** emit parser-internal synthetic nodes such as:

- `CONCAT_EXPR`
- `STRING_BUILD_NODE`
- any other placeholder that has no direct SQL text representation

Only emit tokens directly visible in the SQL text.

#### Rule D — Functions inside `||` do not suppress the underlying source token

If a token is wrapped by a function inside a concatenation, preserve lineage from the underlying direct source token.

Example:

```sql
'cnt=' || CHAR(v_count)
```

Expected direct participating tokens include:

- `CONSTANT:'cnt='`
- `v_count`

If function-level relationships are modeled separately, they may also be emitted, but must not replace the underlying source-token lineage.

#### Rule E — Fallback literals inside `COALESCE` remain direct tokens

Example:

```sql
', region=' || COALESCE(v_region, '')
```

Expected direct participating tokens include:

- `CONSTANT:', region='`
- `v_region`
- `CONSTANT:''`

#### Example 1 — `INSERT ... SELECT` with concatenation

For:

```sql
INSERT INTO A (COL1)
SELECT 'a' || FIELD1
FROM B;
```

expected rows include:

Usage-side rows:

```tsv
PROCEDURE	<source_object>	CONSTANT:'a'	UNKNOWN	UNKNOWN_SELECT_EXPR		SELECT_EXPR	<line_no>	0	SELECT 'a' || FIELD1	PARSER
PROCEDURE	<source_object>	FIELD1	TABLE	B	FIELD1	SELECT_EXPR	<line_no>	1	SELECT 'a' || FIELD1	PARSER
```

Mapping-side rows:

```tsv
PROCEDURE	<source_object>	CONSTANT:'a'	TABLE	A	COL1	INSERT_SELECT_MAP	<line_no>	2	SELECT 'a' || FIELD1	PARSER
PROCEDURE	<source_object>	FIELD1	TABLE	A	COL1	INSERT_SELECT_MAP	<line_no>	3	SELECT 'a' || FIELD1	PARSER
```

#### Example 2 — variable assignment with concatenation

For:

```sql
SET v_msg = 'cnt=' || CHAR(v_count);
```

expected rows include both usage-side rows and mapping-side rows.

Usage-side rows:

```tsv
PROCEDURE	<source_object>	CONSTANT:'cnt='	UNKNOWN	UNKNOWN_SELECT_EXPR		SELECT_EXPR	<line_no>	0	SET v_msg = 'cnt=' || CHAR(v_count);	PARSER
PROCEDURE	<source_object>	v_count	VARIABLE	<source_object>	v_count	SELECT_EXPR	<line_no>	1	SET v_msg = 'cnt=' || CHAR(v_count);	PARSER
```

Mapping-side rows:

```tsv
PROCEDURE	<source_object>	CONSTANT:'cnt='	VARIABLE	<source_object>	v_msg	VARIABLE_SET_MAP	<line_no>	2	SET v_msg = 'cnt=' || CHAR(v_count);	PARSER
PROCEDURE	<source_object>	v_count	VARIABLE	<source_object>	v_msg	VARIABLE_SET_MAP	<line_no>	3	SET v_msg = 'cnt=' || CHAR(v_count);	PARSER
```

#### Validator clarification for `||`

When validating concatenation expressions:

- absence of usage rows for direct participating tokens is a defect
- absence of final mapping rows into the actual target is a defect
- usage rows and mapping rows serve different purposes and must not be treated as duplicates
- absence of parser-internal synthetic concat nodes is **not** a defect

## Relationship filling matrix

### 1. Object-definition relationships

#### `CREATE_VIEW`
Meaning: defines a view object.
- `source_field` = empty
- `target_object_type` = `VIEW`
- `target_object` = created view
- `target_field` = empty

#### `CREATE_TABLE`
Meaning: defines a table-like object, including DB2 session / DGTT-style table objects when applicable.
- `source_field` = empty
- `target_object_type` = resolved created object type
- `target_object` = created object
- `target_field` = empty

For `DECLARE GLOBAL TEMPORARY TABLE`, resolve the created object type as `SESSION_TABLE`.
`WITH REPLACE`, `ON COMMIT PRESERVE ROWS`, `ON COMMIT DELETE ROWS`, and `NOT LOGGED` affect lifecycle semantics but do not change the relationship family.

#### `CREATE_PROCEDURE`
Meaning: defines a stored procedure object.
- `source_field` = empty
- `target_object_type` = `PROCEDURE`
- `target_object` = created procedure
- `target_field` = empty

#### `CREATE_FUNCTION`
Meaning: defines a user-defined function object.
- `source_field` = empty
- `target_object_type` = `FUNCTION`
- `target_object` = created function
- `target_field` = empty

### 2. Object-usage relationships

#### `SELECT_TABLE`
Meaning: statement directly reads a table.
- `source_field` = empty
- `target_object_type` = `TABLE` or `SESSION_TABLE`
- `target_object` = read object
- `target_field` = empty

#### `SELECT_VIEW`
Meaning: statement directly reads a view.
- `source_field` = empty
- `target_object_type` = `VIEW`
- `target_object` = read view
- `target_field` = empty

#### `INSERT_TABLE`
Meaning: statement inserts into an object.
- `source_field` = empty
- `target_object_type` = insert target type
- `target_object` = insert target
- `target_field` = empty

#### `UPDATE_TABLE`
Meaning: statement updates an object.
- `source_field` = empty
- `target_object_type` = updated object type
- `target_object` = updated object
- `target_field` = empty

#### `MERGE_INTO`
Meaning: statement merges into an object.
- `source_field` = empty
- `target_object_type` = merge target type
- `target_object` = merge target
- `target_field` = empty

#### `DELETE_TABLE`
Meaning: statement deletes from a table.
- `source_field` = empty
- `target_object_type` = `TABLE`
- `target_object` = deleted-from table
- `target_field` = empty

#### `DELETE_VIEW`
Meaning: statement deletes from a view.
- `source_field` = empty
- `target_object_type` = `VIEW`
- `target_object` = deleted-from view
- `target_field` = empty

#### `TRUNCATE_TABLE`
Meaning: statement truncates a table.
- `source_field` = empty
- `target_object_type` = `TABLE`
- `target_object` = truncated table
- `target_field` = empty

#### `CALL_FUNCTION`
Meaning: statement directly invokes a function.
- `source_field` = empty
- `target_object_type` = `FUNCTION`
- `target_object` = called function
- `target_field` = empty

#### `CALL_PROCEDURE`
Meaning: statement directly invokes a procedure.
- `source_field` = empty
- `target_object_type` = `PROCEDURE`
- `target_object` = called procedure
- `target_field` = empty

### 3. Target-column declaration relationships

#### `INSERT_TARGET_COL`
Meaning: an `INSERT (...)` target column slot is explicitly declared.
- `source_field` = empty
- `target_object_type` = insert target type
- `target_object` = insert target
- `target_field` = declared target column
- Emit this relationship only when the SQL text contains an explicit `INSERT (col1, col2, ...)` target-column list.

#### `UPDATE_TARGET_COL`
Meaning: a target column is assigned in an `UPDATE SET` clause.
- `source_field` = empty
- `target_object_type` = updated object type
- `target_object` = updated object
- `target_field` = target column being assigned

#### `MERGE_TARGET_COL`
Meaning: a target column is assigned in either branch of a `MERGE`.
- `source_field` = empty
- `target_object_type` = merge target type
- `target_object` = merge target
- `target_field` = target column being updated or inserted

### 4. Direct field-usage relationships

#### `SELECT_FIELD`
Meaning: a resolved field is directly projected in a `SELECT` list as a plain field projection, without additional expression wrapping.
- `source_field` = projected field name
- `target_object_type` = owning object type of that field
- `target_object` = owning object
- `target_field` = same resolved field

#### `SELECT_EXPR`
Meaning: a direct token participates in a `SELECT` expression or non-column select-list item.

Also use this relationship for **usage-side tokens inside scalar procedural expressions**, including `SET` assignment right-hand-side expressions, when a separate mapping relationship such as `VARIABLE_SET_MAP` captures the final target flow.

Use this for:
- fields inside expressions
- literals projected in the select list
- special registers projected in the select list
- computed expressions whose direct operands can be identified
- usage-side tokens inside scalar procedural assignment expressions when the final target flow is represented separately
- `source_field` = participating token (field name or `CONSTANT:<VALUE>`)
- `target_object_type` = owning object type when applicable, otherwise `UNKNOWN`
- `target_object` = owning object when applicable, otherwise a stable placeholder such as `UNKNOWN_SELECT_EXPR`
- `target_field` = participating field when applicable, otherwise empty

#### `UPDATE_SET`
Meaning: a direct RHS token is used in an `UPDATE SET` assignment expression.
- `source_field` = RHS participating token (field name or `CONSTANT:<VALUE>`)
- `target_object_type` = updated target object type
- `target_object` = updated target object
- `target_field` = target column being assigned

### 5. Field-mapping relationships (propagation-capable)

**Precedence note:** More specific mappings take precedence over generic assignment mappings. Emit only the most specific relationship for the same direct semantic role.

Specific relationships include:
- `SPECIAL_REGISTER_MAP`
- `DIAGNOSTICS_FETCH_MAP`
- `FUNCTION_PARAM_MAP`
- `CALL_PARAM_MAP`
- `FUNCTION_EXPR_MAP`

#### `CREATE_VIEW_MAP`
Meaning: source column or direct literal maps into a created view column.
- `source_field` = source column or `CONSTANT:<VALUE>`
- `target_object_type` = `VIEW`
- `target_object` = target view
- `target_field` = target view column

#### `INSERT_SELECT_MAP`
Meaning: source column or direct literal maps into an insert target column.
- `source_field` = source column or `CONSTANT:<VALUE>`
- `target_object_type` = insert target type
- `target_object` = insert target
- `target_field` = insert target column

#### `UPDATE_SET_MAP`
Meaning: source column or direct literal maps into an updated target column.
- `source_field` = source column or `CONSTANT:<VALUE>`
- `target_object_type` = updated target type
- `target_object` = updated target
- `target_field` = updated target column

#### `MERGE_SET_MAP`
Meaning: source column or direct literal maps into a `MERGE` matched-update target column.
- `source_field` = source column or `CONSTANT:<VALUE>`
- `target_object_type` = merge target type
- `target_object` = merge target
- `target_field` = target column

#### `MERGE_INSERT_MAP`
Meaning: source column or direct literal maps into a `MERGE` insert-branch target column.
- `source_field` = source column or `CONSTANT:<VALUE>`
- `target_object_type` = merge target type
- `target_object` = merge target
- `target_field` = target column

#### `VARIABLE_SET_MAP`
Meaning: a source column, parameter, variable, literal, special register, sequence value, or direct expression operand is assigned into a declared variable.
- `source_field` = direct source token, `CONSTANT:<VALUE>`, or `SEQUENCE:<schema.sequence_name>`
- `target_object_type` = `VARIABLE`
- `target_object` = owning routine
- `target_field` = receiving variable name

#### `CURSOR_SELECT_MAP`
Meaning: a direct source token maps into a named cursor result slot defined by `DECLARE CURSOR ... FOR SELECT ...`.
- `source_field` = direct source token, `CONSTANT:<VALUE>`, or `SEQUENCE:<schema.sequence_name>`
- `target_object_type` = `CURSOR`
- `target_object` = cursor name
- `target_field` =
  - explicit select-item alias when present
  - otherwise resolved field name for a naked column select item
  - otherwise positional slot such as `$1`, `$2`, ...

Rules:
- Emit `CURSOR_SELECT_MAP` for each direct value contributor into each cursor result slot.
- If the cursor select item is a naked resolved column reference, `target_field` should normally use the resolved column name.
- If the cursor select item is a computed expression without an alias, use a positional slot.
- Do not collapse base-table source tokens directly into fetched variables when a cursor result slot exists.

#### `CURSOR_FETCH_MAP`
Meaning: a cursor result slot is mapped into a local variable or record slot during `FETCH` or implicit cursor iteration.
- `source_field` = cursor result-slot name or positional slot such as `$1`, `$2`, ...
- `target_object_type` = `VARIABLE`
- `target_object` = owning routine
- `target_field` = receiving variable or record slot
- Prefer qualified slot targets such as `V_REC.DEAL_NUM` when the target record structure is known.
- If only the whole record variable is known, the record variable name may be used as a fallback.

#### `FUNCTION_PARAM_MAP`
Meaning: maps an actual argument into a called function's formal parameter.
- `source_field` = actual argument token (field, variable, or `CONSTANT:<VALUE>`)
- `target_object_type` = `FUNCTION`
- `target_object` = called function
- `target_field` = formal parameter name when known, otherwise positional slot `$1`, `$2`, ...
- Emit for scalar functions and table functions.

#### `CALL_PARAM_MAP`
Meaning: maps an actual argument into a called procedure parameter, or maps a procedure `OUT` / `INOUT` result into a receiving local variable.
- `source_field` = actual argument token, or the procedure `OUT` / `INOUT` parameter token when receiving a returned value
- `target_object_type` =
  - `PROCEDURE` when passing into the callable
  - `VARIABLE` when receiving an `OUT` / `INOUT` value locally
- `target_object` =
  - called procedure name when passing into the callable
  - owning local routine when receiving the returned value
- `target_field` =
  - formal parameter name when known, otherwise positional slot `$1`, `$2`, ...
  - or receiving local variable name for `OUT` / `INOUT` result capture

Additional rules:
- `CALL_PARAM_MAP` is a target-flow relationship only.
- If an actual `CALL` argument is a complex expression, emit usage-side rows for the direct participating tokens inside that argument in addition to the `CALL_PARAM_MAP` row.
- Use the same scalar-expression usage-side relationship family for complex `CALL` arguments that this contract uses for equivalent direct scalar expressions elsewhere; do not invent a `CALL`-specific usage family unless the contract is explicitly revised.
- Under the default profile, use `SELECT_EXPR` as the generic usage-side relationship for scalar-expression tokens inside complex actual arguments unless a stronger usage-side relationship is explicitly required by this contract.

#### `TABLE_FUNCTION_RETURN_MAP`
Meaning: a source column or direct literal maps into a table-valued function `RETURN` table definition.
- `source_field` = source column or `CONSTANT:<VALUE>`
- `target_object_type` = `FUNCTION`
- `target_object` = owning function
- `target_field` = target return column

#### `SPECIAL_REGISTER_MAP`
Meaning: a DB2 special register is directly mapped into a target column or variable.
- `source_field` = `CONSTANT:<SPECIAL_REGISTER>`
- `target_object_type` = target object type (`TABLE`, `VIEW`, or `VARIABLE`)
- `target_object` = target object
- `target_field` = target column or variable

Use this relationship when the special register itself is the direct written value for the target slot.

#### `SEQUENCE_VALUE_MAP`
Meaning: a sequence value obtained through `NEXT VALUE FOR` is directly mapped into a target column or variable.
- `source_field` = `SEQUENCE:<schema.sequence_name>`
- `target_object_type` = target object type (`TABLE`, `VIEW`, or `VARIABLE`)
- `target_object` = target object
- `target_field` = target column or variable

Do **not** encode a sequence reference as `CONSTANT:<VALUE>`.

#### `DIAGNOSTICS_FETCH_MAP`
Meaning: diagnostic information such as `SQLCODE`, `SQLSTATE`, `ROW_COUNT`, `MESSAGE_TEXT`, or other `GET DIAGNOSTICS` properties is fetched into a local variable.
- `source_field` = diagnostic token such as:
  - `CONSTANT:SQLCODE`
  - `CONSTANT:SQLSTATE`
  - `CONSTANT:ROW_COUNT`
  - `CONSTANT:MESSAGE_TEXT`
- `target_object_type` = `VARIABLE`
- `target_object` = owning routine
- `target_field` = receiving variable

Use this relationship both for direct assignments such as `SET v_code = SQLCODE` and for `GET DIAGNOSTICS` forms.

#### `FUNCTION_EXPR_MAP`
Meaning: the outermost function return value is used directly inside a field-level mapping expression.
- `source_field` = called function name
- `target_object_type` = target object type being modified
- `target_object` = target object
- `target_field` = target column or variable

Rules:
- Emit for the outermost named function result that directly lands in the written target slot.
- Do not emit separate target-slot `FUNCTION_EXPR_MAP` rows for every nested wrapper unless a later profile explicitly requires wrapper-level target modeling.
- For scalar `RETURN expr`, use `RETURN_VALUE` rather than `FUNCTION_EXPR_MAP` for direct participating value tokens unless this contract explicitly defines a stronger return-slot mapping profile.

### 6. Predicate / condition relationships

#### `WHERE`
Meaning: a direct token participates in a `WHERE` predicate.
- `source_field` = participating token (field or `CONSTANT:<VALUE>`)
- `target_object_type` = owning object type, or `CURSOR` for `WHERE CURRENT OF cursor_name`
- `target_object` = owning object or cursor name
- `target_field` = participating field when applicable, otherwise empty

#### `JOIN_ON`
Meaning: a direct token participates in a join condition.
Same fill rule as `WHERE`.

#### `MERGE_MATCH`
Meaning: a direct token participates in the `MERGE ... ON ...` match condition.
Same fill rule as `WHERE`.

#### `GROUP_BY`
Meaning: a direct field participates in grouping.
Same fill rule as `WHERE`.

#### `ORDER_BY`
Meaning: a direct token participates in ordering.
Same fill rule as `WHERE`.

#### `HAVING`
Meaning: a direct token participates in a `HAVING` predicate.

Two valid forms are allowed:
1. **Field-level / token-level** when a direct field or literal token can be resolved.
   - `source_field` = participating token
   - `target_object_type` = owning object type when applicable, otherwise `UNKNOWN`
   - `target_object` = owning object when applicable, otherwise `UNKNOWN_AGGREGATE`
   - `target_field` = participating field when applicable, otherwise empty
2. **Object-level fallback** when the expression has no stable direct token representation.
   - `source_field` = empty
   - `target_object_type` = owning object type or `UNKNOWN`
   - `target_object` = owning object or `UNKNOWN_AGGREGATE`
   - `target_field` = empty

Rules:
- Use token-level `HAVING` rows whenever a direct participating token can be stably recovered, including resolved fields, variables, literals / constants, special registers, and any other direct token class explicitly allowed by this contract.
- Use the object-level `UNKNOWN_AGGREGATE` fallback only when the `HAVING` expression has no truthful stable direct token representation under the active extraction profile.
- Do not choose object-level fallback merely because the `HAVING` expression contains aggregate functions if direct underlying participating tokens can still be recovered truthfully.

#### `CONTROL_FLOW_CONDITION`
Meaning: a direct token participates in a procedural control-flow evaluation such as `IF`, `ELSEIF`, searched `CASE`, `WHILE`, `REPEAT`, or loop-exit logic that depends on a boolean condition.
- `source_field` = participating token (variable, field, `CONSTANT:<VALUE>`, or other direct token form allowed by this contract)
- `target_object_type` = owning routine type (`PROCEDURE` or `FUNCTION`)
- `target_object` = owning routine
- `target_field` = participating variable or field when applicable, otherwise empty

Use this relationship for control conditions, not for value-writing target flow.

### Procedural control-structure clarification

Rules:
- Procedural `IF`, `ELSEIF`, searched `CASE` statements, `WHILE`, `REPEAT`, and loop-exit predicates use `CONTROL_FLOW_CONDITION`.
- Value-producing `CASE` expressions do **not** use `CONTROL_FLOW_CONDITION`; they use the usage-side and mapping-side relationships of the surrounding value-expression context.
- Labels, `LEAVE`, `ITERATE`, `END IF`, `END CASE`, `END LOOP`, and similar structural delimiters do not emit standalone lineage rows unless a stronger relationship family in this contract explicitly requires one.
- Statements inside the controlled branch or loop body still emit their own normal lineage rows.

### `FOR` loop normalization

Rules:
- For DB2 `FOR` loop forms, if the loop is cursor-backed or select-backed, emit the cursor / source-read families required by the loop source.
- Do not emit standalone `CONTROL_FLOW_CONDITION` merely because iteration occurs.
- Emit `CONTROL_FLOW_CONDITION` only when the SQL text contains an explicit boolean branch or loop predicate.
- Statements inside the loop body still emit their own normal lineage rows.

### 7. Intermediate / structural relationships

#### `CTE_DEFINE`
Meaning: defines a CTE object.
- `source_field` = empty
- `target_object_type` = `CTE`
- `target_object` = CTE name
- `target_field` = empty

#### `CTE_READ`
Meaning: statement reads a CTE object.
- `source_field` = empty
- `target_object_type` = `CTE`
- `target_object` = CTE name
- `target_field` = empty

#### `UNION_INPUT`
Meaning: the current select branch contributes one direct object input to a `UNION` / `UNION ALL` chain.
- `source_field` = empty
- `target_object_type` = concrete input object type
- `target_object` = concrete input object
- `target_field` = empty

#### `CURSOR_DEFINE`
Meaning: defines a named cursor and its associated `SELECT`.
- `source_field` = empty
- `target_object_type` = `CURSOR`
- `target_object` = cursor name
- `target_field` = empty

#### `CURSOR_READ`
Meaning: statement explicitly opens, fetches from, closes, or otherwise reads from a cursor.
- `source_field` = empty
- `target_object_type` = `CURSOR`
- `target_object` = cursor name
- `target_field` = operation class such as `OPEN`, `FETCH`, `CLOSE`, `OPEN_FOR_STATEMENT`, or another truthful direct cursor operation when recoverable from the SQL text

Rules:
- Emit `CURSOR_READ` for each explicit `OPEN`, `FETCH`, and `CLOSE`.
- Emit `CURSOR_FETCH_MAP` in addition to `CURSOR_READ` for each `FETCH` transfer into local variables or record slots.
- For implicit cursor iteration syntax, emit the equivalent `CURSOR_READ` and `CURSOR_FETCH_MAP` rows as if the fetch operation were explicit.
- Cursor options such as `WITH HOLD`, scrollability, and rowset behavior do not change the relationship family, but may affect `target_field` or validator expectations when DB2 syntax exposes those distinctions directly.

#### `EXCEPTION_HANDLER_MAP`
Meaning: a declared handler explicitly captures or redirects control flow/state based on a DB2 condition.
- `source_field` = caught condition token such as `CONSTANT:SQLEXCEPTION`, `CONSTANT:NOT FOUND`, `CONSTANT:SQLWARNING`, `CONSTANT:'23505'`, or a declared condition name
- `target_object_type` = owning routine type (`PROCEDURE` or `FUNCTION`)
- `target_object` = owning routine
- `target_field` = handler kind such as `EXIT` or `CONTINUE`

Canonicalization rules:
- `FOR SQLEXCEPTION` -> `CONSTANT:SQLEXCEPTION`
- `FOR NOT FOUND` -> `CONSTANT:NOT FOUND`
- `FOR SQLWARNING` -> `CONSTANT:SQLWARNING`
- `FOR SQLSTATE '23505'` -> `CONSTANT:'23505'`
- `FOR <declared_condition_name>` -> the declared condition name itself

Rules:
- Handler declaration scope is the innermost enclosing `BEGIN ... END` block that owns the declaration.
- Statements inside the handler body still emit their own normal lineage rows.
- Use `DIAGNOSTICS_FETCH_MAP` rather than `VARIABLE_SET_MAP` for direct reads of diagnostic state inside the handler body.

#### `CONDITION_DEFINE`
Meaning: defines a named DB2 condition declared through `DECLARE CONDITION`.
- `source_field` = empty
- `target_object_type` = `CONDITION`
- `target_object` = condition name
- `target_field` = empty

### 8. Return / execution relationships

#### `SIGNAL_CONDITION`
Meaning: explicitly raises or re-raises a DB2 condition through `SIGNAL` or `RESIGNAL`.
- `source_field` =
  - declared condition name when present
  - or `CONSTANT:<SQLSTATE>` when SQLSTATE is raised directly
- `target_object_type` = owning routine type (`PROCEDURE` or `FUNCTION`)
- `target_object` = owning routine
- `target_field` = `SIGNAL` or `RESIGNAL`

Rules:
- `SET MESSAGE_TEXT = expr`, `SET SQLSTATE = expr`, or similar signal-option expressions inside `SIGNAL` / `RESIGNAL` must emit the normal usage-side and mapping-side rows required by the surrounding scalar-expression rules.
- Do not collapse a declared condition name into `UNKNOWN` when the SQL text explicitly declares it.

#### `RETURN_VALUE`
Meaning: direct operand-level dependency of a **scalar** function `RETURN` expression.
For table-valued functions, use `TABLE_FUNCTION_RETURN_MAP` instead.

Valid form:
1. **Operand-level token dependency**
   - `source_field` = direct operand token (field, variable, or `CONSTANT:<VALUE>`)
   - `target_object_type` = owning object type when applicable, otherwise `UNKNOWN`
   - `target_object` = owning object when applicable, otherwise `UNKNOWN_RETURN_EXPR`
   - `target_field` = participating field / variable when applicable, otherwise empty

Rules:
- `RETURN_VALUE` records only the final return expression's direct participating value tokens.
- Scalar `RETURN expr` does **not** emit `FUNCTION_EXPR_MAP` into a routine return slot.
- If the return expression calls another function, keep that callable visible through `FUNCTION_PARAM_MAP` rows for the actual arguments and through the operand-level `RETURN_VALUE` rows for the direct participating outer-expression tokens.
- Do not invent a separate callable-dependency row to the called function unless a future profile explicitly introduces one.

Examples:
- `RETURN P_FACTOR * 2;` emits operand rows for `P_FACTOR` and `CONSTANT:2`.
- `RETURN MY_FUNC(V_X);` emits operand-level `RETURN_VALUE` rows required by the surrounding direct-expression policy plus `FUNCTION_PARAM_MAP` rows for `V_X -> MY_FUNC.<param>`.
- `RETURN COALESCE(F1(V_A), F2(V_B), 0);` emits `RETURN_VALUE` only for the direct outer-expression participating tokens and `FUNCTION_PARAM_MAP` rows for arguments into `F1` and `F2`; it does not emit `FUNCTION_EXPR_MAP` into the routine return slot.

### Scalar `RETURN (fullselect)` clarification

Rules:
- For scalar functions, when the `RETURN` expression is a scalar fullselect or scalar subquery, emit the normal object-read and usage-side rows required by the fullselect itself.
- Emit `RETURN_VALUE` rows for the direct final participating value tokens of the scalar return expression.
- Do not emit `FUNCTION_EXPR_MAP` into a scalar routine return slot.
- Do not invent a separate scalar-return target-flow family unless this contract is explicitly revised.

#### `DYNAMIC_SQL_EXEC`
Meaning: dynamic SQL text or a prepared dynamic statement is prepared, executed, or opened through DB2 dynamic-SQL flow such as `EXECUTE IMMEDIATE`, `PREPARE`, `EXECUTE`, or `OPEN cursor FOR statement`.
- `source_field` = participating variable, string literal, or prepared-statement handle directly used in the dynamic flow
- `target_object_type` =
  - `STMT_HANDLE` when a prepared statement handle is explicitly named
  - otherwise `UNKNOWN`
- `target_object` =
  - statement handle name when explicitly declared in the SQL text
  - otherwise `UNKNOWN_DYNAMIC_SQL`
- `target_field` =
  - direct operation class such as `PREPARE`, `EXECUTE`, `EXECUTE_IMMEDIATE`, or `OPEN_CURSOR` when recoverable from the SQL text
  - otherwise empty

Rules:
- If the dynamic SQL text is fully literal and the real target object can be resolved truthfully from the SQL text itself, emit the stronger normal object / field relationships in addition to `DYNAMIC_SQL_EXEC`. These stronger normal rows are mandatory, not optional, whenever truthful static recovery is possible from the available SQL text and metadata.
- Recovered stronger normal rows derived from fully literal dynamic SQL must anchor to the physical source line of the runtime dynamic-SQL statement that executes or prepares the text, such as `EXECUTE IMMEDIATE`, `PREPARE`, `EXECUTE`, or `OPEN ... FOR`.
- Do not create synthetic internal line numbers or synthetic reconstructed `line_content` values for recovered dynamic rows.
- This recovered-dynamic anchoring rule overrides the ordinary target-slot anchoring rule for those recovered rows, even when the recovered statement contains target-slot semantics such as `INSERT_SELECT_MAP`, `UPDATE_SET_MAP`, `MERGE_SET_MAP`, or other target-flow families.
- Use `UNKNOWN_DYNAMIC_SQL` only when truthful object resolution remains impossible from the available SQL text and metadata.

#### `DYNAMIC_SQL_PARAM_MAP`
Meaning: maps an actual `USING` argument into a prepared dynamic statement parameter slot.
- `source_field` = actual argument token directly passed through `USING`
- `target_object_type` =
  - `STMT_HANDLE` when a prepared statement handle is explicitly named
  - otherwise `UNKNOWN`
- `target_object` = statement handle name when known, otherwise `UNKNOWN_DYNAMIC_SQL`
- `target_field` = formal dynamic slot such as `$1`, `$2`, ... in `USING` order

Rules:
- `DYNAMIC_SQL_PARAM_MAP` is a target-flow relationship only.
- If a `USING` argument is a complex scalar expression, emit usage-side rows for the direct participating tokens inside that argument in addition to the `DYNAMIC_SQL_PARAM_MAP` row.
- Under the default profile, use the same generic scalar-expression usage-side relationship family that this contract uses elsewhere for equivalent direct scalar expressions; do not invent a dynamic-SQL-specific usage family unless this contract is explicitly revised.
- Under the default profile, use `SELECT_EXPR` as that generic usage-side relationship unless a stronger usage-side relationship is explicitly required by this contract.
- Direct `USING` parameter binding into prepared dynamic SQL must use `DYNAMIC_SQL_PARAM_MAP`, never `VARIABLE_SET_MAP`, `CALL_PARAM_MAP`, `FUNCTION_PARAM_MAP`, or any generic mapping family for the same direct binding event.

### Rowset / result-set normalization rule

Until this contract defines dedicated rowset/result-set relationship families, DB2 rowset and result-set cursor syntax must normalize into the existing cursor family as follows:
- rowset or result-set cursor declaration -> `CURSOR_DEFINE`
- rowset or result-set source-slot lineage -> `CURSOR_SELECT_MAP`
- rowset or result-set read / open / fetch operation -> `CURSOR_READ`
- rowset or result-set transfer into receiving rowset, array, or record targets -> `CURSOR_FETCH_MAP`

If DB2 syntax explicitly exposes rowset/result-set distinctions, capture that distinction in `target_field` normalization rather than inventing a new ad hoc relationship family.

Canonical `target_field` vocabulary for these normalized cursor operations:
- `OPEN`
- `FETCH`
- `CLOSE`
- `OPEN_DYNAMIC`
- `FETCH_ROWSET`
- `FETCH_RESULT_SET`

Receiving rowset, array, or record targets must still follow logical positional slot order unless a stronger named-slot rule is explicitly defined by this contract.

### Generated / default-value policy

When a target slot receives its value from DB2-generated behavior rather than a direct user-written source token, apply this rule:
- If the SQL text explicitly names the generator, such as `NEXT VALUE FOR`, a special register, or an explicit default expression, emit the strongest direct relationship defined by this contract.
- If the SQL text explicitly uses the `DEFAULT` keyword as the written value, treat it as the direct canonical token `CONSTANT:DEFAULT` and emit the context-appropriate usage-side and target-flow relationships for that explicit token.
- If the value is produced only by schema-side generated behavior not present in the SQL text, such as identity, generated-always, or row-change timestamp behavior, do **not** fabricate a field-level source row.
- Field-level lineage must remain truthful to the SQL text.

### 9. Unknown / unresolved relationships

#### `UNKNOWN`
Meaning: a direct statement exists but cannot yet be expressed as a stronger supported relationship.
- `source_field` = empty unless a direct unresolved token is important for review
- `target_object_type` = `UNKNOWN`
- `target_object` = one of:
  - `UNKNOWN_UNSUPPORTED_STATEMENT`
  - `UNKNOWN_UNRESOLVED_OBJECT`
  - another stable, truthful placeholder of the same class
- `target_field` = empty

## Explicit contract scope boundary

This contract is normative for these source-object owners unless a broader profile is explicitly enabled:
- `VIEW_DDL`
- `FUNCTION`
- `PROCEDURE`
- `SCRIPT`

Source-object kinds outside this scope, such as triggers or other DB2 executable objects, must not be silently normalized into an arbitrary in-scope owner type.
They must either:
- be rejected by scope policy,
- or be introduced through an explicit future contract revision.

## Optional validation guidance

A validator should flag at least these defect classes:

### Core structural defects
- invalid header
- invalid relationship value
- invalid confidence value
- duplicate rows
- inconsistent `line_relation_seq` within a line group
- `line_no` not matching the real SQL file
- `line_content` not matching the real SQL file
- missing mandatory `CURSOR_READ`, `EXCEPTION_HANDLER_MAP`, `CONDITION_DEFINE`, or `SIGNAL_CONDITION` rows when the SQL text explicitly requires them

### Anchoring and ordering defects
- usage-side row anchored to a target-slot line instead of the token line
- target-flow row anchored to a source-token line instead of the receiving-slot line
- `line_relation_seq` not following the contract's slot order or token appearance order
- unstable fallback ordering used when natural SQL order was recoverable
- recovered dynamic-SQL rows anchored by ordinary target-slot rules instead of the contract's recovered-dynamic anchoring override

### Relationship-strength / precedence defects
- weaker relationship emitted where a stronger required relationship exists
- `SELECT_FIELD` emitted for a computed select item
- procedural `CASE` statement condition tokens emitted as `SELECT_EXPR` instead of `CONTROL_FLOW_CONDITION`
- value-producing `CASE` expression condition tokens emitted as `CONTROL_FLOW_CONDITION` instead of the surrounding value-expression usage relationship
- `MERGE ... ON ...` tokens emitted as `WHERE` or `JOIN_ON`
- `VARIABLE_SET_MAP` used for direct diagnostic-state reads that require `DIAGNOSTICS_FETCH_MAP`
- generic `*_MAP` used where `SPECIAL_REGISTER_MAP`, `SEQUENCE_VALUE_MAP`, `FUNCTION_PARAM_MAP`, `CALL_PARAM_MAP`, or `FUNCTION_EXPR_MAP` is required
- avoidable duplicate strong-and-weak rows emitted for the same direct semantic fact
- nested wrapper `FUNCTION_EXPR_MAP` rows over-emitted when only the outermost direct function-result row is allowed
- generic `UNKNOWN` used where explicit dynamic-SQL flow requires `DYNAMIC_SQL_EXEC`
- generic branch-local mapping emitted for a direct special-register or sequence target flow that requires `SPECIAL_REGISTER_MAP` or `SEQUENCE_VALUE_MAP`

### Mapping completeness defects
- missing mandatory usage rows
- missing mandatory mapping rows
- missing cursor result-slot rows before `CURSOR_FETCH_MAP`
- direct collapse from base table source to fetched variable when a cursor slot should have been modeled
- missing slot-aligned rows for `SELECT ... INTO`, `VALUES ... INTO`, row-value assignment, or row-value `UPDATE`
- `INSERT_TARGET_COL` emitted without an explicit target-column list
- missing `DYNAMIC_SQL_PARAM_MAP` rows for explicit `USING` bindings
- missing required usage-side rows inside complex `USING` arguments
- cross-line source/target slot misalignment despite recoverable logical slot order

### Token-model defects
- sequence reference encoded as `CONSTANT:<VALUE>` instead of `SEQUENCE:<schema.sequence_name>`
- incorrect literal / `NULL` / special-register / diagnostics token handling
- illegal parser-internal synthetic nodes
- illegal omission of source tokens in `CASE`, `COALESCE`, concatenation, diagnostics, or special-register cases
- non-canonical DB2 special-register spelling used where this contract requires normalized uppercase token form

### Naming and resolution defects
- mixed named and positional parameter targets for the same callable when the signature is known
- inconsistent target-field naming for cursor result slots
- source-object owner kind silently normalized into an arbitrary supported owner type when that owner kind is outside the explicit contract scope
- rowset/result-set syntax not normalized into the required `CURSOR_*` family
- non-canonical `target_field` values used for normalized rowset/result-set cursor operations
- scalar `RETURN (fullselect)` modeled with invented target-flow families not defined by this contract
- unresolved placeholder used where truthful resolution should have been possible from available metadata or SQL text
- fabricated columns or parameter names not supported by the SQL text or metadata
- declared condition name collapsed into `UNKNOWN` when the SQL text explicitly names the condition


## v4.6 closure patch — derived-table alias and emitted operation-class alignment

This revision adds a final closure patch for two remaining implementation-alignment gaps found by cross-checking the latest SQL and TSV against the current contract:
- emitted `EXCEPTION_HANDLER_MAP.target_field` and `CURSOR_READ.target_field` must align with the already-normalized handler / cursor operation classes
- derived-table / inline-view aliases that are explicitly named in SQL text must not fall back to `UNKNOWN_UNRESOLVED_OBJECT` when their alias identity is statically recoverable

### Derived-table / inline-view alias object model rule

When SQL text explicitly introduces a named derived-table or inline-view alias, such as:
- `FROM (SELECT ...) X`
- `USING (SELECT ...) S`
- `JOIN (SELECT ...) D ON ...`

the alias is a truthful recoverable SQL object placeholder for direct relationship purposes.

Use:
- `target_object_type = DERIVED_TABLE`
- `target_object = <alias>`

Rules:
- Do not collapse a statically recoverable derived-table alias into `UNKNOWN_UNRESOLVED_OBJECT`.
- Do not fabricate the underlying base-table object in place of the alias for alias-level join / merge / predicate relationships.
- If a later contract revision introduces a more granular derived-table slot model, that stronger model may refine this behavior, but until then `DERIVED_TABLE` is the required normalization.

### Handler emitted target-field rule

For `EXCEPTION_HANDLER_MAP`, `target_field` is mandatory and must encode the declared handler kind:
- `EXIT`
- `CONTINUE`

Examples:
- `DECLARE EXIT HANDLER FOR SQLEXCEPTION` -> `target_field = EXIT`
- `DECLARE CONTINUE HANDLER FOR NOT FOUND` -> `target_field = CONTINUE`

An empty `target_field` for `EXCEPTION_HANDLER_MAP` is a validation defect.

### Cursor emitted target-field rule

For `CURSOR_READ`, `target_field` is mandatory and must encode the cursor operation class.
Canonical values include:
- `OPEN`
- `FETCH`
- `CLOSE`
- `OPEN_DYNAMIC`
- `FETCH_ROWSET`
- `FETCH_RESULT_SET`

An empty `target_field` for `CURSOR_READ` is a validation defect.

### Additional precedence clarification for derived-table aliases

For `JOIN_ON`, `MERGE_MATCH`, `WHERE`, and similar direct predicate families:
- when one side of the predicate is an explicitly named derived-table alias, prefer that alias placeholder (`DERIVED_TABLE`, alias name) over `UNKNOWN_UNRESOLVED_OBJECT`
- only use unresolved placeholders when the alias itself cannot be truthfully recovered from the SQL text

### Additional validator defects

A validator should also flag at least these defect classes:
- `EXCEPTION_HANDLER_MAP` emitted with empty `target_field`
- `CURSOR_READ` emitted with empty `target_field`
- derived-table / inline-view alias collapsed into `UNKNOWN_UNRESOLVED_OBJECT` even though the alias name is explicitly present in the SQL text

### Source-side alias / object disambiguation rule

For field-level relationships, `source_field` must preserve the minimal truthful source-side identity needed to disambiguate the direct token.

Rules:
- If the direct token is unambiguous as a plain field name under the active statement scope, the plain field token may be used.
- If two or more direct source objects in the same statement scope can supply the same field name, preserve source-side alias identity in canonical form such as `<alias>.<field>`.
- This rule applies especially to joins, `MERGE USING` aliases, derived-table aliases, cursor source slots, and correlated subqueries.
- Do not discard source-side alias identity when doing so would make the direct source token ambiguous.


### Family-level source-side alias preservation

The global source-side alias rule above is mandatory for the following relationship families whenever a collapsed plain field token would become ambiguous or would hide the truthful immediate source object in the active statement scope:

- `MERGE_MATCH`
- `MERGE_SET_MAP`
- `MERGE_INSERT_MAP`
- `UPDATE_SET`
- `UPDATE_SET_MAP`
- `INSERT_SELECT_MAP`
- `VARIABLE_SET_MAP`
- `RETURN_VALUE`

Rules:
- When the direct source token comes from an explicitly named `DERIVED_TABLE` alias, preserve canonical `<alias>.<field>` form in `source_field` whenever the consuming statement directly references that alias.
- When a join, `MERGE USING`, correlated subquery, or cursor source exposes same-name fields from multiple source objects, preserve canonical `<alias>.<field>` form in `source_field` for the direct participating token.
- Do not collapse `S.EVENT_COUNT` to `EVENT_COUNT` in `MERGE_SET_MAP` or `MERGE_INSERT_MAP` when `S` is the truthful direct source alias seen by the consuming `MERGE`.
- Do not collapse source-side alias identity merely because deeper provenance inside the derived table could be computed separately; the consuming-statement-level direct source must remain truthful first.

### Recovered dynamic-SQL ordering clarification

When one physical runtime dynamic-SQL line recovers multiple stronger normal rows, determine `line_relation_seq` from the recovered SQL statement's logical order, then project that stable logical order onto the runtime line.

Rules:
- first determine recovered SQL family-bucket order using the same precedence used for static SQL on that recovered statement
- within each recovered family, preserve target-slot order when the family is slot-oriented
- within the same recovered slot, preserve direct token appearance order
- only after those three steps project the recovered ordering onto the single runtime line that owns the recovered rows
- do not attempt to infer ordering from character positions inside the runtime string variable or string literal alone
- do not use arbitrary dictionary sorting when recovered SQL logical order is truthfully recoverable
- recovered dynamic-SQL ordering must still respect the contract's family bucket order and slot / token order rules

### Reserved status of `ROWSET` and `RESULT_SET`

`ROWSET` and `RESULT_SET` are currently reserved object-type vocabulary for future contract expansion.

Until dedicated rowset/result-set relationship families are explicitly introduced:
- emitted rowset/result-set behavior must normalize into the `CURSOR_*` family
- do not emit ad hoc dedicated rowset/result-set relationship families
- do not rely on `ROWSET` or `RESULT_SET` alone to bypass cursor-family normalization

### `FOR` loop normalization rule

For DB2 `FOR` loop forms:
- cursor-backed or select-backed iteration emits the cursor / source-read families required by the loop source
- iteration alone does not emit `CONTROL_FLOW_CONDITION`
- emit `CONTROL_FLOW_CONDITION` only when the SQL text contains an explicit boolean branch or loop predicate
- statements inside the loop body emit their own normal lineage rows

### Additional validator defects

A validator should also flag at least these defect classes:
- source-side alias/object identity omitted when required for truthful disambiguation
- `MERGE USING` / join / correlated-subquery source tokens collapsed to ambiguous plain field names even though canonical alias-qualified identity was truthfully recoverable
- recovered dynamic-SQL rows ordered by arbitrary fallback instead of recovered SQL logical order
- recovered dynamic-SQL rows ordered without preserving recovered family bucket order, then slot order, then token order when that logical order was recoverable
- `ROWSET` or `RESULT_SET` emitted in place of required cursor-family normalization before this contract defines dedicated relationship families

### v4.7 closure patch — formal enumeration alignment

`DERIVED_TABLE` is a formally allowed `target_object_type` value and must appear in the common-values enumeration wherever `target_object_type` values are listed.



### v4.8 closure patch — final production-grade closure

This closure patch adds the final remaining contract-tightening items identified during cross-review of the latest MD, SQL, and TSV:
- source-side alias/object disambiguation when plain field tokens would be ambiguous
- recovered dynamic-SQL logical ordering projected onto the runtime statement line
- reserved-status clarification for `ROWSET` and `RESULT_SET` before dedicated families exist
- explicit `FOR` loop normalization rules
- validator defects for source-side alias loss and non-canonical recovered-dynamic ordering
- family-level source-side alias preservation requirements for `MERGE_*`, `UPDATE_SET_MAP`, `INSERT_SELECT_MAP`, and `VARIABLE_SET_MAP`
- a more mechanical recovered dynamic-SQL ordering recipe for validator-friendly implementation
- explicit reserved-only status alignment for `ROWSET` and `RESULT_SET` in the object-type enumeration


### v4.9 closure patch — alias fidelity and recovered ordering

This revision tightens the remaining implementation-divergence edges by:
- pushing source-side alias preservation down into key relationship families
- making recovered dynamic-SQL ordering more mechanical and validator-friendly
- aligning the object-type enumeration with the reserved-only status of `ROWSET` and `RESULT_SET`


### Normative examples for source-side alias preservation

#### Example 1 — `MERGE USING` derived-table alias

For:

```sql
MERGE INTO TGT T
USING (
    SELECT A.ID, A.EVENT_COUNT
      FROM SRC_A A
) S
ON T.ID = S.ID
WHEN MATCHED THEN
    UPDATE SET T.EVENT_COUNT = S.EVENT_COUNT;
```

required behavior:
- `MERGE_MATCH` preserves `S.ID` in `source_field`
- `MERGE_SET_MAP` preserves `S.EVENT_COUNT` in `source_field`
- do **not** collapse `S.EVENT_COUNT` to plain `EVENT_COUNT` at the consuming `MERGE` statement level

#### Example 2 — joined same-name source fields

For:

```sql
SELECT A.STATUS, B.STATUS
  INTO v1, v2
  FROM T1 A
  JOIN T2 B
    ON A.ID = B.ID;
```

required behavior:
- preserve `A.STATUS` and `B.STATUS` as distinct direct source tokens whenever collapsed plain `STATUS` would be ambiguous in the active statement scope

### Concrete validator examples for alias-loss defects

The validator should treat the following as defects when alias identity is truthfully recoverable from the SQL text:
- `S.EVENT_COUNT` collapsed to `EVENT_COUNT` in `MERGE_SET_MAP`
- `S.BIZ_DATE` collapsed to `BIZ_DATE` in `MERGE_MATCH`
- `A.STATUS` and `B.STATUS` both collapsed to `STATUS` in a joined `SELECT ... INTO`
- correlated-subquery outer references collapsed to plain field tokens when alias-qualified identity was recoverable and required for truthful disambiguation

### Recovered dynamic-SQL ordering example

For:

```sql
EXECUTE IMMEDIATE 'INSERT INTO T (A, B) SELECT X, Y FROM S';
```

required recovered ordering on the runtime line:
1. `DYNAMIC_SQL_EXEC`
2. recovered `INSERT_TABLE`
3. recovered `INSERT_TARGET_COL` for `A`, then `B`
4. recovered `SELECT_TABLE`
5. recovered `INSERT_SELECT_MAP` for `X -> A`, then `Y -> B`

Rules:
- when recovered logical order is truthfully recoverable, the validator must reject arbitrary fallback ordering
- family order comes before slot order, and slot order comes before token order

### v5.0 closure patch — normative examples and validator sharpeners

This revision adds the final practical closure items identified during end-to-end review of the latest MD, SQL, and TSV:
- normative `MERGE USING (...) S` examples for source-side alias preservation
- explicit joined-field alias-disambiguation examples
- concrete validator examples for alias-loss defects
- a concrete recovered dynamic-SQL ordering example that makes family → slot → token ordering directly testable


## v5.1 closure patch

This closure patch tightens the last remaining alignment issues observed when validating the latest sample SQL and TSV against this contract.

### Strong alias-preservation enforcement note

The alias-preservation rule is not merely advisory.

When canonical `<alias>.<field>` form is required for truthful disambiguation:
- emitting the collapsed plain field token is a contract violation
- validators must treat the collapsed form as a defect
- current TSV compatibility does not override this rule

### Normative `MERGE USING (...) S` alias example

For:

```sql
MERGE INTO TEMP.TEST_EVENT_DAILY_SUMMARY T
USING (
    SELECT ld_biz_date AS BIZ_DATE,
           p_product_type AS PRODUCT_TYPE,
           p_deal_type AS DEAL_TYPE,
           COUNT(*) AS EVENT_COUNT
      FROM TEMP.TEST_EVENT_MASTER_P MAST
) S
ON T.BIZ_DATE = S.BIZ_DATE
WHEN MATCHED THEN
    UPDATE SET T.EVENT_COUNT = S.EVENT_COUNT
WHEN NOT MATCHED THEN
    INSERT (BIZ_DATE, PRODUCT_TYPE, DEAL_TYPE, EVENT_COUNT)
    VALUES (S.BIZ_DATE, S.PRODUCT_TYPE, S.DEAL_TYPE, S.EVENT_COUNT);
```

Required behavior:
- preserve `T.BIZ_DATE` and `S.BIZ_DATE` in `MERGE_MATCH`
- preserve `T.PRODUCT_TYPE` and `S.PRODUCT_TYPE` in `MERGE_MATCH`
- preserve `T.DEAL_TYPE` and `S.DEAL_TYPE` in `MERGE_MATCH`
- preserve `S.EVENT_COUNT` in `MERGE_SET_MAP`
- preserve `S.BIZ_DATE`, `S.PRODUCT_TYPE`, `S.DEAL_TYPE`, and `S.EVENT_COUNT` in `MERGE_INSERT_MAP`
- do not collapse those source tokens to plain field names at the consuming `MERGE` statement level

### Concrete validator examples for alias-loss

The validator should treat at least the following as defects:
- `S.BIZ_DATE` collapsed to `BIZ_DATE` in `MERGE_MATCH`
- `S.EVENT_COUNT` collapsed to `EVENT_COUNT` in `MERGE_SET_MAP`
- `S.PRODUCT_TYPE` collapsed to `PRODUCT_TYPE` in `MERGE_INSERT_MAP`
- `A.STATUS` and `B.STATUS` both collapsed to `STATUS` in a joined `SELECT ... INTO`

### Clarification for reserved-only object-type enumeration

`ROWSET` and `RESULT_SET` may appear in the object-type vocabulary list for forward compatibility, but this does not authorize emitting them as active object types until dedicated relationship families are introduced.

## v5.2 closure note

This revision is an ultra-final wording polish only.
It does not change the semantic contract shape introduced by v5.1.
It only:
- makes reserved-only vocabulary warnings more visible
- adds slightly more concrete validator example wording for alias-loss
- makes recovered dynamic-SQL ordering wording a bit more mechanical

No new relationship family is introduced in v5.2.
