package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SetOperationList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class SelectStatementExtractor implements StatementExtractor {
    @Override
    public boolean supports(ParsedStatementResult parsedStatement) {
        return parsedStatement.statement().filter(Select.class::isInstance).isPresent();
    }

    @Override
    public void extract(ParsedStatementResult parsedStatement, ExtractionContext context, RowCollector collector) {
        Select select = (Select) parsedStatement.statement().orElseThrow();
        int naturalOrder = 0;
        List<String> cteNames = ObjectRelationshipSupport.collectCteNames(select);
        Set<String> cteNamesUpper = new HashSet<>();

        for (String cteName : cteNames) {
            cteNamesUpper.add(cteName.toUpperCase());
            collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                    context, parsedStatement, RelationshipType.CTE_DEFINE, TargetObjectType.CTE, cteName, naturalOrder++
            ));
        }

        for (ObjectRelationshipSupport.TableRef ref : ObjectRelationshipSupport.collectSelectReadObjects(select)) {
            RelationshipType relationship = RelationshipType.SELECT_TABLE;
            TargetObjectType targetType = ref.targetType();
            if (cteNamesUpper.contains(ref.objectName().toUpperCase())) {
                relationship = RelationshipType.CTE_READ;
                targetType = TargetObjectType.CTE;
            }
            collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                    context, parsedStatement, relationship, targetType, ref.objectName(), naturalOrder++
            ));
        }

        if (select instanceof SetOperationList setOperationList && setOperationList.getSelects() != null) {
            for (net.sf.jsqlparser.statement.select.Select branch : setOperationList.getSelects()) {
                if (branch instanceof PlainSelect plainSelect) {
                    if (plainSelect.getFromItem() != null) {
                        String objectName = plainSelect.getFromItem().toString();
                        collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                                context, parsedStatement, RelationshipType.UNION_INPUT, TargetObjectType.TABLE, objectName, naturalOrder++
                        ));
                    }
                }
            }
        }
    }
}
