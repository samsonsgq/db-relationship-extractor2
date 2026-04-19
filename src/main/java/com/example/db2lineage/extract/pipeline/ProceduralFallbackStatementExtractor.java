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
                || parsedStatement.statement().isEmpty()
                || isParsedButProceduralCandidate(parsedStatement);
    }

    @Override
    public void extract(ParsedStatementResult parsedStatement, ExtractionContext context, RowCollector collector) {
        if (ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()) == com.example.db2lineage.model.SourceObjectType.FUNCTION
                && parsedStatement.slice().statementText().stripLeading().toUpperCase().startsWith("RETURN")) {
            return;
        }
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
        if (RoutineLineageSupport.isTransactionControlStatement(parsedStatement.slice().statementText())) {
            return;
        }
        if (RoutineLineageSupport.isPlainVariableDeclaration(parsedStatement.slice().statementText())) {
            return;
        }
        if (RoutineLineageSupport.isRoutineStructuralStatement(parsedStatement.slice().statementText())) {
            return;
        }

        String dynamicToken = ObjectRelationshipSupport.extractDynamicSqlSourceToken(parsedStatement.slice().statementText());
        if (!dynamicToken.isBlank()) {
            String normalizedDynamicToken = dynamicToken.trim();
            if (normalizedDynamicToken.startsWith("'") && normalizedDynamicToken.endsWith("'")) {
                normalizedDynamicToken = "CONSTANT:" + normalizedDynamicToken;
            }
            LineAnchorResolver.LineAnchor anchor = LineAnchorResolver.token(parsedStatement.slice(), "EXECUTE IMMEDIATE", 0);
            collector.addDraft(new RowDraft(
                    ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                    ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                    normalizedDynamicToken,
                    TargetObjectType.UNKNOWN,
                    ObjectRelationshipSupport.UNKNOWN_DYNAMIC_SQL,
                    "",
                    RelationshipType.DYNAMIC_SQL_EXEC,
                    anchor.lineNo(),
                    anchor.lineContent(),
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

    private static boolean isParsedButProceduralCandidate(ParsedStatementResult parsedStatement) {
        if (parsedStatement.statement().isEmpty()) {
            return false;
        }
        String first = parsedStatement.slice().statementText().stripLeading().toUpperCase();
        return first.startsWith("DECLARE ")
                || first.startsWith("SET ");
    }
}
