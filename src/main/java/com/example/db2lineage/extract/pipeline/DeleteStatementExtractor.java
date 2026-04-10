package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.WithItem;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class DeleteStatementExtractor implements StatementExtractor {
    @Override
    public boolean supports(ParsedStatementResult parsedStatement) {
        return parsedStatement.statement().filter(Delete.class::isInstance).isPresent();
    }

    @Override
    public void extract(ParsedStatementResult parsedStatement, ExtractionContext context, RowCollector collector) {
        Delete delete = (Delete) parsedStatement.statement().orElseThrow();
        String targetName = delete.getTable() == null ? null : delete.getTable().getFullyQualifiedName();
        collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                context,
                parsedStatement,
                RelationshipType.DELETE_TABLE,
                TargetObjectType.TABLE,
                targetName,
                0
        ));

        int naturalOrder = 1;
        Set<String> cteUpper = new HashSet<>();
        List<WithItem<?>> withItems = delete.getWithItemsList();
        if (withItems != null) {
            for (WithItem<?> withItem : withItems) {
                String cteName = withItem.getAliasName();
                if (cteName != null && !cteName.isBlank()) {
                    cteUpper.add(cteName.toUpperCase());
                    collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                            context, parsedStatement, RelationshipType.CTE_DEFINE, TargetObjectType.CTE, cteName, naturalOrder++
                    ));
                }
            }
        }

        if (delete.getUsingList() != null) {
            for (net.sf.jsqlparser.schema.Table usingTable : delete.getUsingList()) {
                String objectName = usingTable.getFullyQualifiedName();
                RelationshipType relationship = cteUpper.contains(objectName.toUpperCase())
                        ? RelationshipType.CTE_READ
                        : RelationshipType.SELECT_TABLE;
                TargetObjectType targetType = cteUpper.contains(objectName.toUpperCase())
                        ? TargetObjectType.CTE
                        : TargetObjectType.TABLE;
                collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                        context,
                        parsedStatement,
                        relationship,
                        targetType,
                        objectName,
                        naturalOrder++
                ));
            }
        }
    }
}
