package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import net.sf.jsqlparser.statement.create.procedure.CreateProcedure;

import java.util.List;

public final class CreateProcedureStatementExtractor implements StatementExtractor {
    @Override
    public boolean supports(ParsedStatementResult parsedStatement) {
        return parsedStatement.statement().filter(CreateProcedure.class::isInstance).isPresent();
    }

    @Override
    public void extract(ParsedStatementResult parsedStatement, ExtractionContext context, RowCollector collector) {
        CreateProcedure createProcedure = (CreateProcedure) parsedStatement.statement().orElseThrow();
        String name = extractName(createProcedure.getFunctionDeclarationParts());
        collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                context,
                parsedStatement,
                RelationshipType.CREATE_PROCEDURE,
                TargetObjectType.PROCEDURE,
                name,
                0
        ));
        RoutineBodyStatementSupport.extractNestedStatements(parsedStatement, context, collector);
        RoutineLineageSupport.extractProceduralLinesFromSlice(parsedStatement, context, collector);
    }

    private String extractName(List<String> declarationParts) {
        if (declarationParts == null || declarationParts.isEmpty()) {
            return ObjectRelationshipSupport.UNKNOWN_UNRESOLVED_OBJECT;
        }
        return declarationParts.get(0);
    }
}
