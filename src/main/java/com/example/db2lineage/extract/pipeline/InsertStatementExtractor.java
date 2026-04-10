package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class InsertStatementExtractor implements StatementExtractor {
    @Override
    public boolean supports(ParsedStatementResult parsedStatement) {
        return parsedStatement.statement().filter(Insert.class::isInstance).isPresent();
    }

    @Override
    public void extract(ParsedStatementResult parsedStatement, ExtractionContext context, RowCollector collector) {
        Insert insert = (Insert) parsedStatement.statement().orElseThrow();
        String targetName = insert.getTable() == null ? null : insert.getTable().getFullyQualifiedName();
        collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                context,
                parsedStatement,
                RelationshipType.INSERT_TABLE,
                TargetObjectType.TABLE,
                targetName,
                0
        ));

        Select select = insert.getSelect();
        if (select == null) {
            return;
        }

        int naturalOrder = 1;
        List<String> cteNames = ObjectRelationshipSupport.collectCteNames(select);
        Set<String> cteUpper = new HashSet<>();
        for (String cteName : cteNames) {
            cteUpper.add(cteName.toUpperCase());
            collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                    context, parsedStatement, RelationshipType.CTE_DEFINE, TargetObjectType.CTE, cteName, naturalOrder++
            ));
        }
        for (ObjectRelationshipSupport.TableRef ref : ObjectRelationshipSupport.collectSelectReadObjects(select)) {
            RelationshipType relationship = RelationshipType.SELECT_TABLE;
            TargetObjectType targetType = ref.targetType();
            if (cteUpper.contains(ref.objectName().toUpperCase())) {
                relationship = RelationshipType.CTE_READ;
                targetType = TargetObjectType.CTE;
            }
            collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                    context,
                    parsedStatement,
                    relationship,
                    targetType,
                    ref.objectName(),
                    naturalOrder++
            ));
        }
    }
}
