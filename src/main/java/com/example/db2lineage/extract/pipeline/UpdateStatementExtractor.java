package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;

import java.util.HashSet;
import java.util.List;
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
                if (updateSet.getValues() == null) {
                    continue;
                }
                for (Object value : updateSet.getValues()) {
                    if (value instanceof Expression expression) {
                        ExpressionTokenSupport.addExpressionRows(RelationshipType.UPDATE_SET, expression, parsedStatement, context, collector, naturalOrder++);
                    }
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
}
