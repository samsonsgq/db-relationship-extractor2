package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.ConfidenceLevel;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.RowDraft;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;

import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class UpdateStatementExtractor implements StatementExtractor {
    @Override
    public boolean supports(ParsedStatementResult parsedStatement) {
        return parsedStatement.statement().filter(Update.class::isInstance).isPresent();
    }

    @Override
    public void extract(ParsedStatementResult parsedStatement, ExtractionContext context, RowCollector collector) {
        Update update = (Update) parsedStatement.statement().orElseThrow();
        String targetName = update.getTable() == null ? null : update.getTable().getFullyQualifiedName();
        collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                context,
                parsedStatement,
                RelationshipType.UPDATE_TABLE,
                TargetObjectType.TABLE,
                targetName,
                0
        ));

        int naturalOrder = 1;
        Set<String> cteUpper = new HashSet<>();
        List<WithItem<?>> withItems = update.getWithItemsList();
        if (withItems != null) {
            for (WithItem<?> withItem : withItems) {
                String cteName = withItem.getAliasName();
                if (cteName == null || cteName.isBlank()) {
                    continue;
                }
                cteUpper.add(cteName.toUpperCase());
                collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                        context, parsedStatement, RelationshipType.CTE_DEFINE, TargetObjectType.CTE, cteName, naturalOrder++
                ));
            }
        }

        java.util.LinkedHashSet<ObjectRelationshipSupport.TableRef> refs = new java.util.LinkedHashSet<>();
        ObjectRelationshipSupport.collectFromItemObjects(update.getFromItem(), refs);
        if (update.getJoins() != null) {
            for (Join join : update.getJoins()) {
                ObjectRelationshipSupport.collectFromItemObjects(join.getFromItem(), refs);
            }
        }
        for (ObjectRelationshipSupport.TableRef ref : refs) {
            RelationshipType relationship = RelationshipType.SELECT_TABLE;
            TargetObjectType targetType = ref.targetType();
            if (cteUpper.contains(ref.objectName().toUpperCase())) {
                relationship = RelationshipType.CTE_READ;
                targetType = TargetObjectType.CTE;
            }
            collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                    context, parsedStatement, relationship, targetType, ref.objectName(), naturalOrder++
            ));
        }

        if (update.getUpdateSets() != null) {
            for (UpdateSet updateSet : update.getUpdateSets()) {
                if (updateSet.getColumns() != null) {
                    for (Column column : updateSet.getColumns()) {
                        MappingRelationshipSupport.addTargetColumnRow(
                                RelationshipType.UPDATE_TARGET_COL,
                                targetName,
                                TargetObjectType.TABLE,
                                column.getColumnName(),
                                parsedStatement,
                                context,
                                collector,
                                naturalOrder++
                        );
                    }
                }
                if (updateSet.getValues() == null) {
                    continue;
                }
                int colIndex = 0;
                for (Object value : updateSet.getValues()) {
                    int consumedColumns = 1;
                    if (value instanceof Expression expression) {
                        ExpressionTokenSupport.addExpressionRows(RelationshipType.UPDATE_SET, expression, parsedStatement, context, collector, naturalOrder++);
                        String targetField = updateSet.getColumns() != null && colIndex < updateSet.getColumns().size()
                                ? updateSet.getColumns().get(colIndex).getColumnName() : "";
                        if (!targetField.isBlank()) {
                            MappingRelationshipSupport.addConciseMappingRows(
                                    RelationshipType.UPDATE_SET_MAP,
                                    targetName,
                                    TargetObjectType.TABLE,
                                    targetField,
                                    expression,
                                    parsedStatement,
                                    context,
                                    collector,
                                    naturalOrder++
                            );
                        }
                    } else if (value instanceof ParenthesedSelect parenthesedSelect) {
                        consumedColumns = addUpdateSelectAssignmentRows(
                                parenthesedSelect.getSelect(),
                                updateSet,
                                colIndex,
                                targetName,
                                parsedStatement,
                                context,
                                collector,
                                naturalOrder
                        );
                        naturalOrder += 10_000;
                    } else if (value instanceof Select selectValue) {
                        consumedColumns = addUpdateSelectAssignmentRows(
                                selectValue,
                                updateSet,
                                colIndex,
                                targetName,
                                parsedStatement,
                                context,
                                collector,
                                naturalOrder
                        );
                        naturalOrder += 10_000;
                    }
                    colIndex += Math.max(1, consumedColumns);
                }
            }
        }

        ExpressionTokenSupport.addExpressionRows(RelationshipType.WHERE, update.getWhere(), parsedStatement, context, collector, naturalOrder++);

        if (update.getJoins() != null) {
            for (Join join : update.getJoins()) {
                if (join.getOnExpressions() != null) {
                    for (Expression onExpression : join.getOnExpressions()) {
                        ExpressionTokenSupport.addExpressionRows(RelationshipType.JOIN_ON, onExpression, parsedStatement, context, collector, naturalOrder++);
                    }
                } else {
                    ExpressionTokenSupport.addExpressionRows(RelationshipType.JOIN_ON, join.getOnExpression(), parsedStatement, context, collector, naturalOrder++);
                }
            }
        }
    }

    private int addUpdateSelectAssignmentRows(Select selectValue,
                                              UpdateSet updateSet,
                                              int startColIndex,
                                              String targetName,
                                              ParsedStatementResult parsedStatement,
                                              ExtractionContext context,
                                              RowCollector collector,
                                              int naturalOrderBase) {
        if (selectValue == null) {
            return 1;
        }
        int naturalOrder = naturalOrderBase;
        for (ObjectRelationshipSupport.TableRef ref : ObjectRelationshipSupport.collectSelectReadObjects(selectValue)) {
            collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                    context,
                    parsedStatement,
                    RelationshipType.SELECT_TABLE,
                    ref.targetType(),
                    ref.objectName(),
                    naturalOrder++
            ));
        }
        PlainSelect plainSelect = selectValue.getPlainSelect();
        if (plainSelect == null || plainSelect.getSelectItems() == null) {
            return 1;
        }
        int mappedCount = 0;
        for (SelectItem<?> selectItem : plainSelect.getSelectItems()) {
            Expression expression = selectItem.getExpression();
            if (expression == null) {
                continue;
            }
            if (expression instanceof Column column) {
                addSelectFieldRow(column, plainSelect, parsedStatement, context, collector, naturalOrder++);
            } else {
                ExpressionTokenSupport.addExpressionRows(RelationshipType.SELECT_EXPR, expression, parsedStatement, context, collector, naturalOrder++);
            }
            String targetField = updateSet.getColumns() != null && startColIndex + mappedCount < updateSet.getColumns().size()
                    ? updateSet.getColumns().get(startColIndex + mappedCount).getColumnName()
                    : "";
            if (!targetField.isBlank()) {
                MappingRelationshipSupport.addConciseMappingRows(
                        RelationshipType.UPDATE_SET_MAP,
                        targetName,
                        TargetObjectType.TABLE,
                        targetField,
                        expression,
                        parsedStatement,
                        context,
                        collector,
                        naturalOrder++
                );
            }
            mappedCount++;
        }
        ExpressionTokenSupport.addExpressionRows(RelationshipType.WHERE, plainSelect.getWhere(), parsedStatement, context, collector, naturalOrder);
        return Math.max(1, mappedCount);
    }

    private void addSelectFieldRow(Column column,
                                   PlainSelect plainSelect,
                                   ParsedStatementResult parsedStatement,
                                   ExtractionContext context,
                                   RowCollector collector,
                                   int naturalOrder) {
        String sourceField = column.getColumnName();
        String qualified = column.getFullyQualifiedName();
        if (qualified != null && !qualified.isBlank() && qualified.contains(".")) {
            sourceField = qualified;
        } else if (sourceField == null || sourceField.isBlank()) {
            sourceField = column.getFullyQualifiedName();
        }
        String qualifier = column.getTable() == null ? "" : column.getTable().getName();
        TableOwner owner = resolveTableOwner(plainSelect, qualifier, context);
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

    private TableOwner resolveTableOwner(PlainSelect plainSelect,
                                         String qualifier,
                                         ExtractionContext context) {
        Map<String, String> aliasToObject = new LinkedHashMap<>();
        collectOwner(plainSelect.getFromItem(), aliasToObject);
        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
                collectOwner(join.getFromItem(), aliasToObject);
            }
        }
        if (qualifier != null && !qualifier.isBlank()) {
            String obj = aliasToObject.getOrDefault(qualifier.toUpperCase(Locale.ROOT), qualifier);
            TargetObjectType type = context.schemaMetadataService().resolveObjectType(obj).orElse(TargetObjectType.TABLE);
            return new TableOwner(type, obj);
        }
        if (aliasToObject.size() == 1) {
            String obj = aliasToObject.values().iterator().next();
            TargetObjectType type = context.schemaMetadataService().resolveObjectType(obj).orElse(TargetObjectType.TABLE);
            return new TableOwner(type, obj);
        }
        return new TableOwner(TargetObjectType.UNKNOWN, ObjectRelationshipSupport.UNKNOWN_UNRESOLVED_OBJECT);
    }

    private void collectOwner(FromItem fromItem, Map<String, String> aliasToObject) {
        if (!(fromItem instanceof Table table)) {
            return;
        }
        String name = table.getFullyQualifiedName();
        if (name == null || name.isBlank()) {
            return;
        }
        aliasToObject.put(name.toUpperCase(Locale.ROOT), name);
        if (table.getAlias() != null && table.getAlias().getName() != null) {
            aliasToObject.put(table.getAlias().getName().toUpperCase(Locale.ROOT), name);
        }
    }

    private record TableOwner(TargetObjectType targetType, String targetObject) {
    }
}
