package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.ConfidenceLevel;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.RowDraft;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
public final class ProceduralFallbackStatementExtractor implements StatementExtractor {
    @Override
    public boolean supports(ParsedStatementResult parsedStatement) {
        return parsedStatement.parseFailed()
                || parsedStatement.unsupported()
                || parsedStatement.statement().isEmpty();
    }

    @Override
    public void extract(ParsedStatementResult parsedStatement, ExtractionContext context, RowCollector collector) {
        int baseOrder = 0;
        boolean foundRoutineLineage = false;
        for (int i = 0; i < parsedStatement.slice().rawLines().size(); i++) {
            String line = parsedStatement.slice().rawLines().get(i);
            int lineNo = parsedStatement.slice().startLine() + i;
            if (RoutineLineageSupport.extractLine(line, lineNo, parsedStatement, context, collector, baseOrder)) {
                foundRoutineLineage = true;
            }
            baseOrder += 100;
        }
        if (foundRoutineLineage) {
            return;
        }

        if (RoutineLineageSupport.extractSetAssignment(
                parsedStatement.slice().statementText(),
                ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                parsedStatement,
                context,
                collector,
                0
        )) {
            return;
        }

        String dynamicToken = ObjectRelationshipSupport.extractDynamicSqlSourceToken(parsedStatement.slice().statementText());
        if (!dynamicToken.isBlank()) {
            collector.addDraft(new RowDraft(
                    ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                    ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                    dynamicToken,
                    TargetObjectType.UNKNOWN,
                    ObjectRelationshipSupport.UNKNOWN_DYNAMIC_SQL,
                    "",
                    RelationshipType.DYNAMIC_SQL_EXEC,
                    parsedStatement.slice().startLine(),
                    ObjectRelationshipSupport.firstLine(parsedStatement.slice()),
                    ConfidenceLevel.DYNAMIC_LOW_CONFIDENCE,
                    ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                    0
            ));
            return;
        }

        collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                context,
                parsedStatement,
                RelationshipType.UNKNOWN,
                TargetObjectType.UNKNOWN,
                ObjectRelationshipSupport.UNKNOWN_UNSUPPORTED_STATEMENT,
                0
        ));
    }
}
