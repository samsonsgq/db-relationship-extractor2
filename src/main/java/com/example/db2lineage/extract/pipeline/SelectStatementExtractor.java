package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class SelectStatementExtractor implements StatementExtractor {
    @Override
    public boolean supports(ParsedStatementResult parsedStatement) {
        return parsedStatement.statement().filter(Select.class::isInstance).isPresent();
    }

    @Override
    public void extract(ParsedStatementResult parsedStatement, ExtractionContext context, RowCollector collector) {
        Select select = (Select) parsedStatement.statement().orElseThrow();
        int naturalOrder = 0;
        List<String> cteNames = ObjectRelationshipSupport.collectCteNames(select);
        Set<String> cteNamesUpper = new HashSet<>();

        for (String cteName : cteNames) {
            cteNamesUpper.add(cteName.toUpperCase());
            collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                    context, parsedStatement, RelationshipType.CTE_DEFINE, TargetObjectType.CTE, cteName, naturalOrder++
            ));
        }

        for (ObjectRelationshipSupport.TableRef ref : ObjectRelationshipSupport.collectSelectReadObjects(select)) {
            RelationshipType relationship = RelationshipType.SELECT_TABLE;
            TargetObjectType targetType = ref.targetType();
            if (cteNamesUpper.contains(ref.objectName().toUpperCase())) {
                relationship = RelationshipType.CTE_READ;
                targetType = TargetObjectType.CTE;
            }
            collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                    context, parsedStatement, relationship, targetType, ref.objectName(), naturalOrder++
            ));
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
                    ExpressionTokenSupport.addExpressionRows(RelationshipType.SELECT_FIELD, column, parsedStatement, context, collector, naturalOrder++);
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
}
