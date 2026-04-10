package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

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

        if (!(createView.getSelect() instanceof PlainSelect plainSelect) || plainSelect.getSelectItems() == null) {
            return;
        }

        int naturalOrder = 1;
        for (SelectItem<?> selectItem : plainSelect.getSelectItems()) {
            Expression expression = selectItem.getExpression();
            if (expression == null) {
                continue;
            }
            String targetField = null;
            if (selectItem.getAlias() != null && selectItem.getAlias().getName() != null) {
                targetField = selectItem.getAlias().getName();
            } else if (expression instanceof Column column) {
                targetField = column.getColumnName();
            }
            if (targetField == null || targetField.isBlank()) {
                continue;
            }
            MappingRelationshipSupport.addConciseMappingRows(
                    RelationshipType.CREATE_VIEW_MAP,
                    viewName,
                    TargetObjectType.VIEW,
                    targetField,
                    expression,
                    parsedStatement,
                    context,
                    collector,
                    naturalOrder++
            );
        }
    }
}
