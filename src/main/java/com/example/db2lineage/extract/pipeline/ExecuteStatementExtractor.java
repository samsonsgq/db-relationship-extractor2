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
            collector.addDraft(new RowDraft(
                    ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                    ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                    ObjectRelationshipSupport.extractDynamicSqlSourceToken(sql),
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
                RelationshipType.CALL_PROCEDURE,
                TargetObjectType.PROCEDURE,
                execute.getName(),
                0
        ));
    }
}
