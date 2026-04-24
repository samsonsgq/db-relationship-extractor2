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
import static org.junit.jupiter.api.Assertions.assertTrue;

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


    @Test
    void finalizeRowsUsesBucketOrderBeforeNaturalOrderOnMixedRelationshipFamilies() {
        RowCollector collector = new RowCollector();

        collector.addDraft(new RowDraft(
                SourceObjectType.PROCEDURE,
                "SRC",
                "",
                TargetObjectType.TABLE,
                "T1",
                "",
                RelationshipType.SELECT_TABLE,
                20,
                "SELECT C1 INTO V1 FROM T1",
                ConfidenceLevel.PARSER,
                0,
                5
        ));
        collector.addDraft(new RowDraft(
                SourceObjectType.PROCEDURE,
                "SRC",
                "C1",
                TargetObjectType.TABLE,
                "T1",
                "C1",
                RelationshipType.SELECT_FIELD,
                20,
                "SELECT C1 INTO V1 FROM T1",
                ConfidenceLevel.PARSER,
                0,
                1
        ));
        collector.addDraft(new RowDraft(
                SourceObjectType.PROCEDURE,
                "SRC",
                "C1",
                TargetObjectType.VARIABLE,
                "V1",
                "V1",
                RelationshipType.VARIABLE_SET_MAP,
                20,
                "SELECT C1 INTO V1 FROM T1",
                ConfidenceLevel.PARSER,
                0,
                0
        ));

        List<RelationshipRow> rows = collector.finalizeRows();

        assertEquals(RelationshipType.SELECT_TABLE, rows.get(0).relationship());
        assertEquals(RelationshipType.SELECT_FIELD, rows.get(1).relationship());
        assertEquals(RelationshipType.VARIABLE_SET_MAP, rows.get(2).relationship());
        assertEquals(0, rows.get(0).lineRelationSeq());
        assertEquals(1, rows.get(1).lineRelationSeq());
        assertEquals(2, rows.get(2).lineRelationSeq());
    }

    @Test
    void finalizeRowsPrefersParserWhenRegexAndParserEmitSameSemanticRow() {
        RowCollector collector = new RowCollector();

        collector.addDraft(new RowDraft(
                SourceObjectType.PROCEDURE,
                "RPT.PR_TEST_DEMO",
                "",
                TargetObjectType.TABLE,
                "TEMP.TEST_EVENT_MASTER_P",
                "",
                RelationshipType.SELECT_TABLE,
                897,
                "          FROM TEMP.TEST_EVENT_MASTER_P MAST",
                ConfidenceLevel.REGEX,
                0,
                0
        ));
        collector.addDraft(new RowDraft(
                SourceObjectType.PROCEDURE,
                "RPT.PR_TEST_DEMO",
                "",
                TargetObjectType.TABLE,
                "TEMP.TEST_EVENT_MASTER_P",
                "",
                RelationshipType.SELECT_TABLE,
                897,
                "          FROM TEMP.TEST_EVENT_MASTER_P MAST",
                ConfidenceLevel.PARSER,
                0,
                1
        ));

        List<RelationshipRow> rows = collector.finalizeRows();
        assertEquals(1, rows.size());
        assertEquals(ConfidenceLevel.PARSER, rows.get(0).confidence());
    }

    @Test
    void finalizeRowsDropsRegexWhenParserEquivalentOnlyDiffersByLineContentWhitespace() {
        RowCollector collector = new RowCollector();

        collector.addDraft(new RowDraft(
                SourceObjectType.PROCEDURE,
                "RPT.PR_TEST_DEMO",
                "",
                TargetObjectType.CURSOR,
                "c_event_detail",
                "",
                RelationshipType.CURSOR_DEFINE,
                139,
                "    DECLARE c_event_detail CURSOR FOR",
                ConfidenceLevel.REGEX,
                0,
                0
        ));
        collector.addDraft(new RowDraft(
                SourceObjectType.PROCEDURE,
                "RPT.PR_TEST_DEMO",
                "",
                TargetObjectType.CURSOR,
                "c_event_detail",
                "",
                RelationshipType.CURSOR_DEFINE,
                139,
                "DECLARE c_event_detail CURSOR FOR",
                ConfidenceLevel.PARSER,
                0,
                1
        ));

        List<RelationshipRow> rows = collector.finalizeRows();
        assertEquals(1, rows.size());
        assertEquals(ConfidenceLevel.PARSER, rows.get(0).confidence());
        assertTrue(rows.get(0).lineContent().contains("DECLARE c_event_detail CURSOR FOR"));
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
