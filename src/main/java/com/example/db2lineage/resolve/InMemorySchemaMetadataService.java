package com.example.db2lineage.resolve;

import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.function.CreateFunction;
import net.sf.jsqlparser.statement.create.procedure.CreateProcedure;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class InMemorySchemaMetadataService implements SchemaMetadataService {
    private static final Pattern ROUTINE_SIGNATURE_PREFIX_PATTERN = Pattern.compile(
            "(?is)\\bCREATE\\s+(?:OR\\s+REPLACE\\s+)?(?:FUNCTION|PROCEDURE)\\s+([A-Z0-9_.$\\\"]+)\\s*\\("
    );
    private final Map<String, TargetObjectType> objectTypes;
    private final Map<String, List<String>> objectColumns;
    private final Map<String, CallableSignature> callables;

    private InMemorySchemaMetadataService(Map<String, TargetObjectType> objectTypes,
                                          Map<String, List<String>> objectColumns,
                                          Map<String, CallableSignature> callables) {
        this.objectTypes = Map.copyOf(objectTypes);
        this.objectColumns = Map.copyOf(objectColumns);
        this.callables = Map.copyOf(callables);
    }

    public static InMemorySchemaMetadataService fromParsedStatements(List<ParsedStatementResult> parsedStatements) {
        Map<String, TargetObjectType> types = new LinkedHashMap<>();
        Map<String, List<String>> columns = new LinkedHashMap<>();
        Map<String, CallableSignature> signatures = new LinkedHashMap<>();

        for (ParsedStatementResult parsedStatement : parsedStatements) {
            Optional<Statement> statementOpt = parsedStatement.statement();
            if (statementOpt.isEmpty()) {
                continue;
            }
            Statement statement = statementOpt.get();
            if (statement instanceof CreateTable createTable && createTable.getTable() != null) {
                String tableName = createTable.getTable().getFullyQualifiedName();
                types.put(key(tableName), TargetObjectType.TABLE);
                List<String> cols = new ArrayList<>();
                if (createTable.getColumnDefinitions() != null) {
                    createTable.getColumnDefinitions().forEach(def -> cols.add(def.getColumnName()));
                }
                if (!cols.isEmpty()) {
                    columns.put(key(tableName), List.copyOf(cols));
                }
            } else if (statement instanceof CreateView createView && createView.getView() != null) {
                String viewName = createView.getView().getFullyQualifiedName();
                types.put(key(viewName), TargetObjectType.VIEW);
                List<String> cols = new ArrayList<>();
                Select select = createView.getSelect();
                if (select != null && select.getPlainSelect() != null && select.getPlainSelect().getSelectItems() != null) {
                    for (SelectItem<?> item : select.getPlainSelect().getSelectItems()) {
                        String aliasName = item.getAlias() == null ? null : item.getAlias().getName();
                        if (aliasName != null && !aliasName.isBlank()) {
                            cols.add(aliasName);
                        } else if (item.getExpression() instanceof Column column) {
                            cols.add(column.getColumnName());
                        }
                    }
                }
                if (!cols.isEmpty()) {
                    columns.put(key(viewName), List.copyOf(cols));
                }
            } else if (statement instanceof CreateFunction createFunction) {
                String functionName = first(createFunction.getFunctionDeclarationParts());
                if (functionName != null) {
                    types.put(key(functionName), TargetObjectType.FUNCTION);
                    signatures.put(key(functionName), new CallableSignature(functionName, parseArgumentNames(parsedStatement.slice().statementText())));
                }
            } else if (statement instanceof CreateProcedure createProcedure) {
                String procedureName = first(createProcedure.getFunctionDeclarationParts());
                if (procedureName != null) {
                    types.put(key(procedureName), TargetObjectType.PROCEDURE);
                    signatures.put(key(procedureName), new CallableSignature(procedureName, parseArgumentNames(parsedStatement.slice().statementText())));
                }
            }
        }

        return new InMemorySchemaMetadataService(types, columns, signatures);
    }

    @Override
    public Optional<TargetObjectType> resolveObjectType(String objectName) {
        return Optional.ofNullable(objectTypes.get(key(objectName)));
    }

    @Override
    public List<String> listObjectColumns(String objectName) {
        return objectColumns.getOrDefault(key(objectName), List.of());
    }

    @Override
    public Optional<CallableSignature> resolveCallableSignature(String callableName) {
        CallableSignature signature = callables.get(key(callableName));
        if (signature != null) {
            return Optional.of(signature);
        }
        String normalized = callableName == null ? "" : callableName.trim();
        int dot = normalized.lastIndexOf('.');
        if (dot > 0) {
            return Optional.ofNullable(callables.get(key(normalized.substring(dot + 1))));
        }
        return Optional.empty();
    }

    @Override
    public List<String> resolveTargetColumnListWhenSafelyKnown(String objectName) {
        return listObjectColumns(objectName);
    }

    private static String first(List<String> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.get(0);
    }

    private static String key(String objectName) {
        if (objectName == null) {
            return "";
        }
        return objectName.trim().toUpperCase(Locale.ROOT);
    }

    private static List<String> parseArgumentNames(String statementText) {
        if (statementText == null || statementText.isBlank()) {
            return List.of();
        }
        String argsBlock = extractRoutineArgsBlock(statementText);
        if (argsBlock.isBlank()) {
            return List.of();
        }
        List<String> names = new ArrayList<>();
        for (String rawArg : splitTopLevelCsv(argsBlock)) {
            String cleaned = rawArg.trim().replaceAll("\\s+", " ");
            if (cleaned.isBlank()) {
                continue;
            }
            String[] tokens = cleaned.split(" ");
            int idx = 0;
            while (idx < tokens.length && (
                    tokens[idx].equalsIgnoreCase("IN")
                            || tokens[idx].equalsIgnoreCase("OUT")
                            || tokens[idx].equalsIgnoreCase("INOUT")
            )) {
                idx++;
            }
            if (idx < tokens.length) {
                names.add(tokens[idx].replace("\"", ""));
            }
        }
        return List.copyOf(names);
    }

    private static List<String> splitTopLevelCsv(String value) {
        if (value == null || value.isBlank()) {
            return List.of();
        }
        List<String> items = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '(') {
                depth++;
                current.append(c);
            } else if (c == ')') {
                depth = Math.max(0, depth - 1);
                current.append(c);
            } else if (c == ',' && depth == 0) {
                items.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        if (!current.isEmpty()) {
            items.add(current.toString());
        }
        return List.copyOf(items);
    }

    private static String extractRoutineArgsBlock(String statementText) {
        Matcher matcher = ROUTINE_SIGNATURE_PREFIX_PATTERN.matcher(statementText);
        if (!matcher.find()) {
            return "";
        }
        int start = matcher.end() - 1;
        int depth = 0;
        for (int i = start; i < statementText.length(); i++) {
            char c = statementText.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return statementText.substring(start + 1, i);
                }
            }
        }
        return "";
    }
}
