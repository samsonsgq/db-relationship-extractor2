package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.ConfidenceLevel;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.RowDraft;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SelectStatementExtractor implements StatementExtractor {
    @Override
    public boolean supports(ParsedStatementResult parsedStatement) {
        return parsedStatement.statement().filter(Select.class::isInstance).isPresent();
    }

    @Override
    public void extract(ParsedStatementResult parsedStatement, ExtractionContext context, RowCollector collector) {
        Select select = (Select) parsedStatement.statement().orElseThrow();
        int naturalOrder = 0;
        boolean selectIntoStatement = parsedStatement.slice().statementText().toUpperCase(Locale.ROOT).contains(" INTO ");
        List<String> cteNames = ObjectRelationshipSupport.collectCteNames(select);
        Set<String> cteNamesUpper = new HashSet<>();

        for (String cteName : cteNames) {
            cteNamesUpper.add(cteName.toUpperCase());
            collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                    context, parsedStatement, RelationshipType.CTE_DEFINE, TargetObjectType.CTE, cteName, naturalOrder++
            ));
        }

        if (!selectIntoStatement) {
            for (ObjectRelationshipSupport.TableRef ref : ObjectRelationshipSupport.collectSelectReadObjects(select)) {
                RelationshipType relationship = RelationshipType.SELECT_TABLE;
                TargetObjectType targetType = context.schemaMetadataService()
                        .resolveObjectType(ref.objectName())
                        .orElse(ref.targetType());
                if (targetType == TargetObjectType.VIEW) {
                    relationship = RelationshipType.SELECT_VIEW;
                }
                if (cteNamesUpper.contains(ref.objectName().toUpperCase())) {
                    relationship = RelationshipType.CTE_READ;
                    targetType = TargetObjectType.CTE;
                }
                collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                        context, parsedStatement, relationship, targetType, ref.objectName(), naturalOrder++
                ));
            }
        }

        if (select instanceof SetOperationList setOperationList && setOperationList.getSelects() != null) {
            for (net.sf.jsqlparser.statement.select.Select branch : setOperationList.getSelects()) {
                if (branch instanceof PlainSelect plainSelect) {
                    if (plainSelect.getFromItem() != null) {
                        String objectName = plainSelect.getFromItem().toString();
                        collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                                context, parsedStatement, RelationshipType.UNION_INPUT, TargetObjectType.TABLE, objectName, naturalOrder++
                        ));
                    }
                }
            }
        }

        if (select instanceof PlainSelect plainSelect) {
            naturalOrder = extractFieldUsageFromPlainSelect(parsedStatement, context, collector, plainSelect, naturalOrder);
        }
        if (select instanceof SetOperationList setOperationListWithUsage && setOperationListWithUsage.getSelects() != null) {
            for (net.sf.jsqlparser.statement.select.Select branch : setOperationListWithUsage.getSelects()) {
                if (branch instanceof PlainSelect plainSelect) {
                    naturalOrder = extractFieldUsageFromPlainSelect(parsedStatement, context, collector, plainSelect, naturalOrder);
                }
            }
        }
        if (selectIntoStatement) {
            emitSelectIntoVariableMappings(parsedStatement, context, collector, naturalOrder);
        }

    }

    private void emitSelectIntoVariableMappings(ParsedStatementResult parsedStatement,
                                                ExtractionContext context,
                                                RowCollector collector,
                                                int naturalOrder) {
        String text = parsedStatement.slice().statementText();
        Matcher m = Pattern.compile("(?is)^\\s*SELECT\\s+(.+?)\\s+INTO\\s+(.+?)\\s+FROM\\s+.+$").matcher(text);
        if (!m.find()) {
            return;
        }
        List<String> sourceExprs = splitArgs(m.group(1));
        List<String> targetVars = splitArgs(m.group(2));
        int count = Math.min(sourceExprs.size(), targetVars.size());
        String routineName = ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice());
        for (int i = 0; i < count; i++) {
            String sourceExpr = sourceExprs.get(i).trim();
            String targetVar = targetVars.get(i).trim();
            if (sourceExpr.isBlank() || targetVar.isBlank()) {
                continue;
            }
            String sourceToken = sourceExpr;
            try {
                Expression expr = CCJSqlParserUtil.parseExpression(sourceExpr);
                if (expr instanceof Column col && col.getColumnName() != null && !col.getColumnName().isBlank()) {
                    sourceToken = col.getColumnName();
                }
            } catch (Exception ignored) {
                // Keep original token.
            }
            LineAnchorResolver.LineAnchor anchor = LineAnchorResolver.token(parsedStatement.slice(), sourceExpr, naturalOrder + i);
            collector.addDraft(new RowDraft(
                    ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                    ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                    sourceToken,
                    TargetObjectType.VARIABLE,
                    routineName,
                    targetVar,
                    RelationshipType.VARIABLE_SET_MAP,
                    anchor.lineNo(),
                    anchor.lineContent(),
                    ConfidenceLevel.PARSER,
                    ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                    naturalOrder + i
            ));
        }
    }

    private static List<String> splitArgs(String raw) {
        if (raw == null || raw.isBlank()) {
            return List.of();
        }
        List<String> args = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        boolean inQuote = false;
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c == '\'' && (i + 1 >= raw.length() || raw.charAt(i + 1) != '\'')) {
                inQuote = !inQuote;
                current.append(c);
                continue;
            }
            if (!inQuote) {
                if (c == '(') {
                    depth++;
                } else if (c == ')' && depth > 0) {
                    depth--;
                } else if (c == ',' && depth == 0) {
                    args.add(current.toString().trim());
                    current.setLength(0);
                    continue;
                }
            }
            current.append(c);
        }
        if (!current.isEmpty()) {
            args.add(current.toString().trim());
        }
        return args;
    }

    private int extractFieldUsageFromPlainSelect(ParsedStatementResult parsedStatement,
                                                 ExtractionContext context,
                                                 RowCollector collector,
                                                 PlainSelect plainSelect,
                                                 int naturalOrder) {
        List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
        if (selectItems != null) {
            for (SelectItem<?> selectItem : selectItems) {
                Expression expression = selectItem.getExpression();
                if (expression == null) {
                    continue;
                }
                if (expression instanceof Column column) {
                    addSelectFieldRow(column, parsedStatement, context, collector, plainSelect, naturalOrder++);
                } else {
                    ExpressionTokenSupport.addExpressionRows(RelationshipType.SELECT_EXPR, expression, parsedStatement, context, collector, naturalOrder++);
                }
            }
        }

        ExpressionTokenSupport.addExpressionRows(RelationshipType.WHERE, plainSelect.getWhere(), parsedStatement, context, collector, naturalOrder++);

        List<Join> joins = plainSelect.getJoins();
        if (joins != null) {
            for (Join join : joins) {
                if (join.getOnExpressions() != null) {
                    for (Expression onExpression : join.getOnExpressions()) {
                        ExpressionTokenSupport.addExpressionRows(RelationshipType.JOIN_ON, onExpression, parsedStatement, context, collector, naturalOrder++);
                    }
                } else {
                    ExpressionTokenSupport.addExpressionRows(RelationshipType.JOIN_ON, join.getOnExpression(), parsedStatement, context, collector, naturalOrder++);
                }
            }
        }

        GroupByElement groupByElement = plainSelect.getGroupBy();
        if (groupByElement != null && groupByElement.getGroupByExpressionList() != null) {
            for (Object groupByExpression : groupByElement.getGroupByExpressionList()) {
                if (groupByExpression instanceof Expression expression) {
                    ExpressionTokenSupport.addExpressionRows(RelationshipType.GROUP_BY, expression, parsedStatement, context, collector, naturalOrder++);
                }
            }
        }

        if (plainSelect.getOrderByElements() != null) {
            for (OrderByElement orderByElement : plainSelect.getOrderByElements()) {
                ExpressionTokenSupport.addExpressionRows(RelationshipType.ORDER_BY, orderByElement.getExpression(), parsedStatement, context, collector, naturalOrder++);
            }
        }

        Expression having = plainSelect.getHaving();
        List<ExpressionTokenSupport.TokenUse> havingTokens = ExpressionTokenSupport.collect(having, parsedStatement.slice());
        if (!havingTokens.isEmpty()) {
            for (ExpressionTokenSupport.TokenUse tokenUse : havingTokens) {
                collector.addDraft(new com.example.db2lineage.model.RowDraft(
                        ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                        ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                        tokenUse.token(),
                        TargetObjectType.UNKNOWN,
                        ObjectRelationshipSupport.UNKNOWN_UNRESOLVED_OBJECT,
                        "",
                        RelationshipType.HAVING,
                        tokenUse.lineNo(),
                        tokenUse.lineContent(),
                        com.example.db2lineage.model.ConfidenceLevel.PARSER,
                        ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                        naturalOrder + tokenUse.orderOnLine()
                ));
            }
            naturalOrder++;
        } else if (having != null) {
            collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                    context,
                    parsedStatement,
                    RelationshipType.HAVING,
                    TargetObjectType.UNKNOWN,
                    ObjectRelationshipSupport.UNKNOWN_UNRESOLVED_OBJECT,
                    naturalOrder++
            ));
        }

        return naturalOrder;
    }

    private static void addSelectFieldRow(Column column,
                                          ParsedStatementResult parsedStatement,
                                          ExtractionContext context,
                                          RowCollector collector,
                                          PlainSelect plainSelect,
                                          int naturalOrder) {
        String sourceField = column.getColumnName();
        String qualified = column.getFullyQualifiedName();
        if (qualified != null && !qualified.isBlank() && qualified.contains(".")) {
            sourceField = qualified;
        } else if (sourceField == null || sourceField.isBlank()) {
            sourceField = column.getFullyQualifiedName();
        }
        String qualifier = column.getTable() == null ? "" : column.getTable().getName();

        TableOwner owner = resolveTableOwner(plainSelect, qualifier, context, sourceField);
        LineAnchorResolver.LineAnchor anchor = LineAnchorResolver.token(parsedStatement.slice(), sourceField, naturalOrder);
        collector.addDraft(new RowDraft(
                ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                sourceField,
                owner.targetType(),
                owner.targetObject(),
                column.getColumnName() == null ? sourceField : column.getColumnName(),
                RelationshipType.SELECT_FIELD,
                anchor.lineNo(),
                anchor.lineContent(),
                ConfidenceLevel.PARSER,
                ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                naturalOrder
        ));
    }

    private static TableOwner resolveTableOwner(PlainSelect plainSelect,
                                                String qualifier,
                                                ExtractionContext context,
                                                String sourceField) {
        Map<String, String> aliasToObject = new LinkedHashMap<>();
        collectOwner(plainSelect.getFromItem(), aliasToObject);
        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
                collectOwner(join.getFromItem(), aliasToObject);
            }
        }
        if (qualifier != null && !qualifier.isBlank()) {
            String obj = aliasToObject.getOrDefault(qualifier.toUpperCase(Locale.ROOT), qualifier);
            TargetObjectType t = context.schemaMetadataService().resolveObjectType(obj).orElse(TargetObjectType.TABLE);
            return new TableOwner(t, obj);
        }
        if (aliasToObject.size() == 1) {
            String obj = aliasToObject.values().iterator().next();
            TargetObjectType t = context.schemaMetadataService().resolveObjectType(obj).orElse(TargetObjectType.TABLE);
            return new TableOwner(t, obj);
        }

        return new TableOwner(TargetObjectType.UNKNOWN, ObjectRelationshipSupport.UNKNOWN_UNRESOLVED_OBJECT);
    }

    private static void collectOwner(FromItem fromItem, Map<String, String> aliasToObject) {
        if (fromItem == null) {
            return;
        }
        if (fromItem instanceof Table table) {
            String name = table.getFullyQualifiedName();
            if (name == null || name.isBlank()) {
                return;
            }
            aliasToObject.put(name.toUpperCase(Locale.ROOT), name);
            if (table.getAlias() != null && table.getAlias().getName() != null) {
                aliasToObject.put(table.getAlias().getName().toUpperCase(Locale.ROOT), name);
            }
        }
    }

    private record TableOwner(TargetObjectType targetType, String targetObject) {
    }
}
