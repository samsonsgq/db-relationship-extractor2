package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Values;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

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
        int naturalOrder = 1;
        if (insert.getColumns() != null && !insert.getColumns().isEmpty()) {
            for (var targetColumn : insert.getColumns()) {
                MappingRelationshipSupport.addTargetColumnRow(
                        RelationshipType.INSERT_TARGET_COL,
                        targetName,
                        TargetObjectType.TABLE,
                        targetColumn.getColumnName(),
                        parsedStatement,
                        context,
                        collector,
                        naturalOrder++
                );
            }
        }
        if (select == null) {
            return;
        }

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

        List<String> targetColumns = new java.util.ArrayList<>();
        if (insert.getColumns() != null && !insert.getColumns().isEmpty()) {
            insert.getColumns().forEach(c -> targetColumns.add(c.getColumnName()));
        } else {
            targetColumns.addAll(context.schemaMetadataService().resolveTargetColumnListWhenSafelyKnown(targetName));
        }
        if (select instanceof PlainSelect plainSelect && plainSelect.getSelectItems() != null) {
            List<SelectItem<?>> items = plainSelect.getSelectItems();
            int mappingSlots = Math.min(targetColumns.size(), items.size());
            for (int i = 0; i < mappingSlots; i++) {
                SelectItem<?> item = items.get(i);
                if (item.getExpression() == null) {
                    continue;
                }
                MappingRelationshipSupport.addConciseMappingRows(
                        RelationshipType.INSERT_SELECT_MAP,
                        targetName,
                        TargetObjectType.TABLE,
                        targetColumns.get(i),
                        item.getExpression(),
                        parsedStatement,
                        context,
                        collector,
                    naturalOrder++
                );
            }
            return;
        }

        if (select instanceof Values values && values.getExpressions() != null) {
            int mappingSlots = Math.min(targetColumns.size(), values.getExpressions().size());
            for (int i = 0; i < mappingSlots; i++) {
                var expression = values.getExpressions().get(i);
                if (expression == null) {
                    continue;
                }
                MappingRelationshipSupport.addConciseMappingRows(
                        RelationshipType.INSERT_SELECT_MAP,
                        targetName,
                        TargetObjectType.TABLE,
                        targetColumns.get(i),
                        expression,
                        parsedStatement,
                        context,
                        collector,
                        naturalOrder++
                );
            }
        }
    }
}
