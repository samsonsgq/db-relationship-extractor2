package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import net.sf.jsqlparser.statement.create.function.CreateFunction;

import java.util.List;

public final class CreateFunctionStatementExtractor implements StatementExtractor {
    @Override
    public boolean supports(ParsedStatementResult parsedStatement) {
        return parsedStatement.statement().filter(CreateFunction.class::isInstance).isPresent();
    }

    @Override
    public void extract(ParsedStatementResult parsedStatement, ExtractionContext context, RowCollector collector) {
        CreateFunction createFunction = (CreateFunction) parsedStatement.statement().orElseThrow();
        String name = extractName(createFunction.getFunctionDeclarationParts());
        collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                context,
                parsedStatement,
                RelationshipType.CREATE_FUNCTION,
                TargetObjectType.FUNCTION,
                name,
                0
        ));
    }

    private String extractName(List<String> declarationParts) {
        if (declarationParts == null || declarationParts.isEmpty()) {
            return ObjectRelationshipSupport.UNKNOWN_UNRESOLVED_OBJECT;
        }
        return declarationParts.get(0);
    }
}
