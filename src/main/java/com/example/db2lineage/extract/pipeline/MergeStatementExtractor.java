package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.merge.MergeUpdate;
import net.sf.jsqlparser.statement.select.WithItem;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class MergeStatementExtractor implements StatementExtractor {
    @Override
    public boolean supports(ParsedStatementResult parsedStatement) {
        return parsedStatement.statement().filter(Merge.class::isInstance).isPresent();
    }

    @Override
    public void extract(ParsedStatementResult parsedStatement, ExtractionContext context, RowCollector collector) {
        Merge merge = (Merge) parsedStatement.statement().orElseThrow();
        String target = merge.getTable() == null ? null : merge.getTable().getFullyQualifiedName();
        collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                context,
                parsedStatement,
                RelationshipType.MERGE_INTO,
                TargetObjectType.TABLE,
                target,
                0
        ));

        int naturalOrder = 1;
        Set<String> cteUpper = new HashSet<>();
        List<WithItem<?>> withItems = merge.getWithItemsList();
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
        ObjectRelationshipSupport.collectFromItemObjects(merge.getFromItem(), refs);
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

        ExpressionTokenSupport.addExpressionRows(RelationshipType.MERGE_MATCH, merge.getOnCondition(), parsedStatement, context, collector, naturalOrder++);

        MergeUpdate mergeUpdate = merge.getMergeUpdate();
        if (mergeUpdate != null && mergeUpdate.getUpdateSets() != null) {
            for (var updateSet : mergeUpdate.getUpdateSets()) {
                if (updateSet.getValues() == null) {
                    continue;
                }
                for (Object value : updateSet.getValues()) {
                    if (value instanceof net.sf.jsqlparser.expression.Expression expression) {
                        ExpressionTokenSupport.addExpressionRows(RelationshipType.UPDATE_SET, expression, parsedStatement, context, collector, naturalOrder++);
                    }
                }
            }
        }
    }
}
