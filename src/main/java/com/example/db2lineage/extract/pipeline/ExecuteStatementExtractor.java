package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.ConfidenceLevel;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.RowDraft;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import net.sf.jsqlparser.statement.execute.Execute;

public final class ExecuteStatementExtractor implements StatementExtractor {
    @Override
    public boolean supports(ParsedStatementResult parsedStatement) {
        return parsedStatement.statement().filter(Execute.class::isInstance).isPresent();
    }

    @Override
    public void extract(ParsedStatementResult parsedStatement, ExtractionContext context, RowCollector collector) {
        Execute execute = (Execute) parsedStatement.statement().orElseThrow();
        String sql = parsedStatement.slice().statementText();
        if (sql.toUpperCase().contains("EXECUTE IMMEDIATE")) {
            String sourceToken = ObjectRelationshipSupport.extractDynamicSqlSourceToken(sql);
            sourceToken = canonicalDynamicSourceToken(sourceToken);
            LineAnchorResolver.LineAnchor anchor = LineAnchorResolver.token(parsedStatement.slice(), "EXECUTE IMMEDIATE", 0);
            collector.addDraft(new RowDraft(
                    ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                    ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                    sourceToken,
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
        if (RoutineLineageSupport.extractLine(
                ObjectRelationshipSupport.firstLine(parsedStatement.slice()),
                parsedStatement.slice().startLine(),
                parsedStatement,
                context,
                collector,
                0
        )) {
            return;
        }

        TargetObjectType callableType = context.schemaMetadataService()
                .resolveObjectType(execute.getName())
                .orElse(TargetObjectType.PROCEDURE);
        collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                context,
                parsedStatement,
                callableType == TargetObjectType.FUNCTION ? RelationshipType.CALL_FUNCTION : RelationshipType.CALL_PROCEDURE,
                callableType,
                execute.getName(),
                0
        ));
    }

    private String canonicalDynamicSourceToken(String sourceToken) {
        if (sourceToken == null || sourceToken.isBlank()) {
            return "";
        }
        String trimmed = sourceToken.trim();
        if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
            return "CONSTANT:" + trimmed;
        }
        return trimmed;
    }
}
