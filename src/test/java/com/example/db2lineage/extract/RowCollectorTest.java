package com.example.db2lineage.extract;

import com.example.db2lineage.model.ConfidenceLevel;
import com.example.db2lineage.model.RelationshipRow;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.RowDraft;
import com.example.db2lineage.model.SourceObjectType;
import com.example.db2lineage.model.TargetObjectType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RowCollectorTest {

    @Test
    void finalizeRowsAssignsLineSequenceByNaturalOrderAndDeduplicatesSemantics() {
        RowCollector collector = new RowCollector();

        collector.addDraft(draft("SRC", "B", "TGT1", 10, 2, 5));
        collector.addDraft(draft("SRC", "A", "TGT1", 10, 2, 4));
        collector.addDraft(draft("SRC", "A", "TGT1", 10, 2, 7)); // semantic duplicate
        collector.addDraft(draft("SRC", "C", "TGT2", 11, 3, 1));

        List<RelationshipRow> rows = collector.finalizeRows();

        assertEquals(3, rows.size());

        RelationshipRow firstGroupFirst = rows.get(0);
        RelationshipRow firstGroupSecond = rows.get(1);
        RelationshipRow secondGroup = rows.get(2);

        assertEquals("A", firstGroupFirst.sourceField());
        assertEquals(0, firstGroupFirst.lineRelationSeq());

        assertEquals("B", firstGroupSecond.sourceField());
        assertEquals(1, firstGroupSecond.lineRelationSeq());

        assertEquals("C", secondGroup.sourceField());
        assertEquals(0, secondGroup.lineRelationSeq());
    }

    private static RowDraft draft(
            String sourceObject,
            String sourceField,
            String targetObject,
            int lineNo,
            int statementOrder,
            int naturalOrderOnLine
    ) {
        return new RowDraft(
                SourceObjectType.PROCEDURE,
                sourceObject,
                sourceField,
                TargetObjectType.TABLE,
                targetObject,
                "TARGET_COL",
                RelationshipType.UPDATE_SET_MAP,
                lineNo,
                "SET TARGET_COL = SOURCE_COL",
                ConfidenceLevel.PARSER,
                statementOrder,
                naturalOrderOnLine
        );
    }
}
