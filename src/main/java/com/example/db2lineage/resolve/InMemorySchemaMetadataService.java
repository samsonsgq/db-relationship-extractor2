package com.example.db2lineage.resolve;

import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.function.CreateFunction;
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

public final class InMemorySchemaMetadataService implements SchemaMetadataService {
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
                    signatures.put(key(functionName), new CallableSignature(functionName, List.of()));
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
        return Optional.ofNullable(callables.get(key(callableName)));
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
}
