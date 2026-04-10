package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import net.sf.jsqlparser.statement.create.view.CreateView;

public final class CreateViewStatementExtractor implements StatementExtractor {
    @Override
    public boolean supports(ParsedStatementResult parsedStatement) {
        return parsedStatement.statement().filter(CreateView.class::isInstance).isPresent();
    }

    @Override
    public void extract(ParsedStatementResult parsedStatement, ExtractionContext context, RowCollector collector) {
        CreateView createView = (CreateView) parsedStatement.statement().orElseThrow();
        String viewName = createView.getView() == null ? null : createView.getView().getFullyQualifiedName();
        collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                context,
                parsedStatement,
                RelationshipType.CREATE_VIEW,
                TargetObjectType.VIEW,
                viewName,
                0
        ));
    }
}
