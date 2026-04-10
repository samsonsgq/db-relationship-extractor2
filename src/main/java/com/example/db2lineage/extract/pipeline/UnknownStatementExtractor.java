package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;

public final class UnknownStatementExtractor implements StatementExtractor {
    @Override
    public boolean supports(ParsedStatementResult parsedStatement) {
        return parsedStatement.statement().isPresent();
    }

    @Override
    public void extract(ParsedStatementResult parsedStatement, ExtractionContext context, RowCollector collector) {
        if (ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()) == com.example.db2lineage.model.SourceObjectType.FUNCTION
                && parsedStatement.slice().statementText().stripLeading().toUpperCase().startsWith("RETURN ")) {
            return;
        }
        boolean foundRoutineLineage = false;
        for (int i = 0; i < parsedStatement.slice().rawLines().size(); i++) {
            String line = parsedStatement.slice().rawLines().get(i);
            int lineNo = parsedStatement.slice().startLine() + i;
            if (RoutineLineageSupport.extractLine(line, lineNo, parsedStatement, context, collector, i * 100)) {
                foundRoutineLineage = true;
            }
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
