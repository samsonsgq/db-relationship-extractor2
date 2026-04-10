# relationship_detail.tsv — revised semantics and filling matrix (DB2-focused, schema-aware)

This document is the **semantic contract** for generating and validating `relationship_detail.tsv`.

It is intended to stay aligned with the current sample SQL and TSV, but it is **authoritative over the TSV** when the two diverge. Any mismatch between this document and a generated TSV should be treated as a defect in either:
- the extractor / parser / post-processor, or
- this contract itself.

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

### Exact source fidelity
- `line_no` must be the **exact physical source line number** from the SQL file.
- `line_content` must be the **exact original raw source line** from the SQL file.
- If a row's semantics are correct but the `line_no` or `line_content` is wrong, the row is still considered defective.

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
  - `CONSTANT:USER`

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
- Use `DIAGNOSTICS_FETCH_MAP` for diagnostic-state fetches.
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
- `FUNCTION`
- `PROCEDURE`
- `CURSOR`
- `VARIABLE`
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
   - `INSERT_SELECT_MAP`, `TABLE_FUNCTION_RETURN_MAP`: select-item / mapping slot order
   - `UPDATE_TARGET_COL`: `SET` target assignment order
   - `UPDATE_SET_MAP`: target assignment order, then source-expression appearance order if more than one direct source participates
   - `MERGE_TARGET_COL`, `MERGE_SET_MAP`, `MERGE_INSERT_MAP`: branch-local target / mapping slot order
   - `CREATE_VIEW_MAP`: view select-list order
   - `FUNCTION_PARAM_MAP`, `CALL_PARAM_MAP`: callable argument order
   - `VARIABLE_SET_MAP`, `SPECIAL_REGISTER_MAP`, `DIAGNOSTICS_FETCH_MAP`, `FUNCTION_EXPR_MAP`: target assignment / slot order
3. For direct field-usage and predicate rows, preserve token appearance order on the source line:
   - `SELECT_FIELD`, `SELECT_EXPR`, `WHERE`, `JOIN_ON`, `GROUP_BY`, `HAVING`, `ORDER_BY`, `MERGE_MATCH`, `UPDATE_SET`, `CONTROL_FLOW_CONDITION`, `RETURN_VALUE`
4. For object-level rows, preserve statement encounter order on that line:
   - `SELECT_TABLE`, `SELECT_VIEW`, `INSERT_TABLE`, `UPDATE_TABLE`, `DELETE_TABLE`, `DELETE_VIEW`, `TRUNCATE_TABLE`, `MERGE_INTO`, `CALL_FUNCTION`, `CALL_PROCEDURE`, `CTE_DEFINE`, `CTE_READ`, `UNION_INPUT`, `CREATE_VIEW`, `CREATE_TABLE`, `CREATE_PROCEDURE`, `CREATE_FUNCTION`, `UNKNOWN`, `CURSOR_DEFINE`, `CURSOR_READ`, `DYNAMIC_SQL_EXEC`, `EXCEPTION_HANDLER_MAP`
5. Only use a deterministic fallback sort when the parser cannot recover meaningful natural order. In that fallback case, sort by:
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

## Recommended Mapping Granularity for Complex Expressions

For complex expressions, keep a clear separation between:

- **usage relationships** that explain which tokens participate in the expression
- **mapping relationships** that identify the main value sources that land in the target

### Usage-side coverage

For complex `SELECT`, `INSERT ... SELECT`, `UPDATE SET`, `MERGE`, and similar expressions:

- use `SELECT_EXPR`, `UPDATE_SET`, `WHERE`, `JOIN_ON`, `HAVING`, `CONTROL_FLOW_CONDITION`, etc.
- these relationships may include all direct participating tokens:
  - columns
  - variables
  - parameters
  - literals
  - DB2 special registers
  - function-call usage tokens when appropriate

This provides full explainability of how the expression is built.

### Mapping-side coverage

For propagation-capable mapping relationships such as:

- `INSERT_SELECT_MAP`
- `UPDATE_SET_MAP`
- `MERGE_SET_MAP`
- `MERGE_INSERT_MAP`

prefer a **concise value-source mapping set**.

#### Preferred rule

Emit mapping rows mainly for tokens that directly contribute to the resulting value written into the target column.

Examples of preferred mapping sources:

- arithmetic value inputs such as `AMT`, `TAX`, `RATE`
- string-building value inputs such as `FIRST_NAME`, `LAST_NAME`
- alternative value branches such as `X` and `Y` in `COALESCE(X, Y, 0)`
- true source values in `CASE` result branches such as `THEN X ELSE Z`

#### Usually do not emit mapping rows for

Unless the implementation has a strong reason to do so, do **not** expand mapping rows to every control token in a complex expression, such as:

- branch-control columns used only for predicates
- comparison-only columns
- boolean-condition columns
- discriminator flags
- structural literals used only to control logic
- list-membership constants such as `IN (1,2,5,6)` when they do not themselves contribute value to the target

These tokens should still appear in usage relationships like `SELECT_EXPR`, `WHERE`, `HAVING`, or `CONTROL_FLOW_CONDITION`, but they usually should **not** produce extra `*_MAP` rows.

### Practical clarity rule

For one complex target expression, it is often clearer to emit:

- **many usage rows** for explainability
- but only **one or a small number of mapping rows** for the main value sources

This avoids overloading the TSV with noisy mappings while preserving both:

- explainability
- direct lineage usefulness

### Example

For:

```sql
INSERT INTO T_TARGET (CHARGE_AMOUNT)
SELECT
  SUM(CASE WHEN REC_SEQ_NUM IN (1,2,5,6) AND BROKER_CLNT_FLAG = 'C'
           THEN AMT_BFORE_GST
           ELSE 0 END)
+ SUM(CASE WHEN BROKER_CLNT_FLAG = 'B'
           THEN AMT_BFORE_GST - AMT_AFTER_GST
           ELSE (AMT_AFTER_GST - AMT_BFORE_GST) END)
FROM S;
```

Recommended extraction style:

- usage rows may include:
  - `REC_SEQ_NUM`
  - `BROKER_CLNT_FLAG`
  - `AMT_BFORE_GST`
  - `AMT_AFTER_GST`
  - relevant constants such as `'C'`, `'B'`, `0`, `1`, `2`, `5`, `6`

- mapping rows should preferably focus on the main value contributors:
  - `AMT_BFORE_GST -> T_TARGET.CHARGE_AMOUNT`
  - `AMT_AFTER_GST -> T_TARGET.CHARGE_AMOUNT`

This keeps the result readable without losing the important direct value lineage.

### Short policy statement

For complex expressions, usage relationships may capture all direct participating tokens, but `INSERT_SELECT_MAP`, `UPDATE_SET_MAP`, `MERGE_SET_MAP`, and `MERGE_INSERT_MAP` should preferably capture only the main direct value contributors to the written target, not every control or predicate token.

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
Use this for:
- fields inside expressions
- literals projected in the select list
- special registers projected in the select list
- computed expressions whose direct operands can be identified
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
Meaning: a source column, parameter, variable, literal, or direct expression operand is assigned into a declared variable.
- `source_field` = direct source token or `CONSTANT:<VALUE>`
- `target_object_type` = `VARIABLE`
- `target_object` = owning routine
- `target_field` = receiving variable name

#### `CURSOR_FETCH_MAP`
Meaning: a source column from a cursor is mapped into a local variable or record slot during `FETCH` or implicit cursor iteration.
- `source_field` = cursor column name
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

#### `DIAGNOSTICS_FETCH_MAP`
Meaning: diagnostic information such as `SQLCODE`, `SQLSTATE`, or `GET DIAGNOSTICS` properties is fetched into a local variable.
- `source_field` = diagnostic token such as `CONSTANT:SQLSTATE`
- `target_object_type` = `VARIABLE`
- `target_object` = owning routine
- `target_field` = receiving variable

#### `FUNCTION_EXPR_MAP`
Meaning: a function return value is used directly inside a field-level mapping expression.
- `source_field` = called function name
- `target_object_type` = target object type being modified
- `target_object` = target object
- `target_field` = target column or variable

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

#### `CONTROL_FLOW_CONDITION`
Meaning: a direct token participates in a procedural control-flow evaluation such as `IF`, `WHILE`, `CASE`, or `REPEAT`.
- `source_field` = participating token (variable, field, or `CONSTANT:<VALUE>`)
- `target_object_type` = owning routine type (`PROCEDURE` or `FUNCTION`)
- `target_object` = owning routine
- `target_field` = participating variable or field when applicable, otherwise empty

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
- `target_field` = empty

#### `EXCEPTION_HANDLER_MAP`
Meaning: an exception handler explicitly captures or redirects control flow/state based on an error condition.
- `source_field` = caught condition token such as `CONSTANT:SQLEXCEPTION`, `CONSTANT:NOT FOUND`, `CONSTANT:'02000'`
- `target_object_type` = owning routine type (`PROCEDURE` or `FUNCTION`)
- `target_object` = owning routine
- `target_field` = empty

### 8. Return / execution relationships

#### `RETURN_VALUE`
Meaning: direct operand-level dependency of a **scalar** function `RETURN` expression.
For table-valued functions, use `TABLE_FUNCTION_RETURN_MAP` instead.

Valid forms:
1. **Operand-level token dependency**
   - `source_field` = direct operand token (field, variable, or `CONSTANT:<VALUE>`)
   - `target_object_type` = owning object type when applicable, otherwise `UNKNOWN`
   - `target_object` = owning object when applicable, otherwise `UNKNOWN_RETURN_EXPR`
   - `target_field` = participating field / variable when applicable, otherwise empty
2. **Callable dependency**
   - `source_field` = empty
   - `target_object_type` = `FUNCTION`
   - `target_object` = called function
   - `target_field` = empty

Examples:
- `RETURN P_FACTOR * 2;` may emit:
  - operand row for `P_FACTOR`
  - operand row for `CONSTANT:2`
- `RETURN MY_FUNC(V_X);` may emit:
  - callable dependency row to `MY_FUNC`
  - plus `FUNCTION_PARAM_MAP` rows for `V_X -> MY_FUNC.<param>`

#### `DYNAMIC_SQL_EXEC`
Meaning: a variable or literal containing dynamic SQL is executed, such as `EXECUTE IMMEDIATE`.
- `source_field` = participating variable or string literal
- `target_object_type` = `UNKNOWN`
- `target_object` = `UNKNOWN_DYNAMIC_SQL`
- `target_field` = empty

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

## Optional validation guidance

A validator should flag at least these defect classes:
- invalid header
- invalid relationship value
- invalid confidence value
- duplicate rows
- inconsistent `line_relation_seq` within a line group
- `line_no` not matching the real SQL file
- `line_content` not matching the real SQL file
- rows that violate explicit contract rules such as:
  - `INSERT_TARGET_COL` emitted without an explicit target-column list
  - `SELECT_FIELD` emitted for a literal projection
  - mixed named and positional parameter targets for the same callable when the signature is known
