package com.example.db2lineage.emit;

import com.example.db2lineage.model.ConfidenceLevel;
import com.example.db2lineage.model.RelationshipRow;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.SourceObjectType;
import com.example.db2lineage.model.TargetObjectType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RelationshipDetailTsvWriterTest {

    @TempDir
    Path tempDir;

    @Test
    void headerMatchesContractColumns() {
        assertEquals(
                "source_object_type\tsource_object\tsource_field\ttarget_object_type\ttarget_object\ttarget_field\trelationship\tline_no\tline_relation_seq\tline_content\tconfidence",
                RelationshipDetailTsvWriter.HEADER
        );
        assertEquals(RelationshipDetailTsvWriter.HEADER, RelationshipRow.TSV_HEADER);
    }

    @Test
    void rowSerializesToTsvInExactColumnOrder() {
        RelationshipRow row = row(
                SourceObjectType.VIEW_DDL,
                "INTERFACE.V_SAMPLE",
                "SRC_COL",
                TargetObjectType.TABLE,
                "INTERFACE.T_SAMPLE",
                "TGT_COL",
                RelationshipType.INSERT_SELECT_MAP,
                42,
                1,
                "INSERT INTO INTERFACE.T_SAMPLE (TGT_COL) SELECT SRC_COL FROM INTERFACE.SRC",
                ConfidenceLevel.PARSER
        );

        assertEquals(
                "VIEW_DDL\tINTERFACE.V_SAMPLE\tSRC_COL\tTABLE\tINTERFACE.T_SAMPLE\tTGT_COL\tINSERT_SELECT_MAP\t42\t1\tINSERT INTO INTERFACE.T_SAMPLE (TGT_COL) SELECT SRC_COL FROM INTERFACE.SRC\tPARSER",
                row.toTsvLine()
        );
    }

    @Test
    void strictValidationRejectsInvalidRows() {
        assertThrows(IllegalArgumentException.class,
                () -> row(SourceObjectType.SCRIPT, " ", "", TargetObjectType.UNKNOWN, "UNKNOWN_DYNAMIC_SQL", "", RelationshipType.UNKNOWN, 1, 0, "EXECUTE IMMEDIATE V_SQL", ConfidenceLevel.DYNAMIC_LOW_CONFIDENCE));

        assertThrows(IllegalArgumentException.class,
                () -> row(SourceObjectType.SCRIPT, "EXTRA_PATTERNS", "", TargetObjectType.UNKNOWN, "UNKNOWN_DYNAMIC_SQL", "", RelationshipType.UNKNOWN, 0, 0, "EXECUTE IMMEDIATE V_SQL", ConfidenceLevel.DYNAMIC_LOW_CONFIDENCE));

        assertThrows(IllegalArgumentException.class,
                () -> row(SourceObjectType.SCRIPT, "EXTRA_PATTERNS", "", TargetObjectType.UNKNOWN, "UNKNOWN_DYNAMIC_SQL", "", RelationshipType.UNKNOWN, 1, -1, "EXECUTE IMMEDIATE V_SQL", ConfidenceLevel.DYNAMIC_LOW_CONFIDENCE));

        assertThrows(IllegalArgumentException.class,
                () -> row(SourceObjectType.SCRIPT, "EXTRA_PATTERNS", "bad\tfield", TargetObjectType.UNKNOWN, "UNKNOWN_DYNAMIC_SQL", "", RelationshipType.UNKNOWN, 1, 0, "EXECUTE IMMEDIATE V_SQL", ConfidenceLevel.DYNAMIC_LOW_CONFIDENCE));
    }

    @Test
    void writesRowsInDeterministicStableOrder() throws Exception {
        RelationshipRow later = row(SourceObjectType.SCRIPT, "Z_OBJ", "", TargetObjectType.TABLE, "T2", "", RelationshipType.SELECT_TABLE, 8, 0, "SELECT * FROM T2", ConfidenceLevel.PARSER);
        RelationshipRow earlier = row(SourceObjectType.SCRIPT, "A_OBJ", "", TargetObjectType.TABLE, "T1", "", RelationshipType.SELECT_TABLE, 2, 0, "SELECT * FROM T1", ConfidenceLevel.PARSER);

        Path outputFile = tempDir.resolve("relationship_detail.tsv");
        new RelationshipDetailTsvWriter().write(outputFile, List.of(later, earlier));

        List<String> lines = Files.readAllLines(outputFile);
        assertEquals(RelationshipDetailTsvWriter.HEADER, lines.get(0));
        assertEquals(earlier.toTsvLine(), lines.get(1));
        assertEquals(later.toTsvLine(), lines.get(2));
    }

    @Test
    void writeToOutputDirOverwritesDeterministically() throws Exception {
        RelationshipDetailTsvWriter writer = new RelationshipDetailTsvWriter();
        RelationshipRow first = row(SourceObjectType.FUNCTION, "F_OBJ", "", TargetObjectType.VARIABLE, "F_OBJ", "V_ONE", RelationshipType.VARIABLE_SET_MAP, 3, 0, "SET V_ONE = 1", ConfidenceLevel.PARSER);
        RelationshipRow second = row(SourceObjectType.FUNCTION, "F_OBJ", "", TargetObjectType.VARIABLE, "F_OBJ", "V_TWO", RelationshipType.VARIABLE_SET_MAP, 4, 0, "SET V_TWO = 2", ConfidenceLevel.PARSER);

        Path outputFile = writer.writeToOutputDir(tempDir, List.of(first, second));
        List<String> initial = Files.readAllLines(outputFile);
        assertEquals(3, initial.size());

        writer.writeToOutputDir(tempDir, List.of(second));
        List<String> overwritten = Files.readAllLines(outputFile);
        assertEquals(2, overwritten.size());
        assertEquals(RelationshipDetailTsvWriter.HEADER, overwritten.get(0));
        assertEquals(second.toTsvLine(), overwritten.get(1));
    }

    private static RelationshipRow row(
            SourceObjectType sourceObjectType,
            String sourceObject,
            String sourceField,
            TargetObjectType targetObjectType,
            String targetObject,
            String targetField,
            RelationshipType relationship,
            int lineNo,
            int lineRelationSeq,
            String lineContent,
            ConfidenceLevel confidence
    ) {
        return new RelationshipRow(
                sourceObjectType,
                sourceObject,
                sourceField,
                targetObjectType,
                targetObject,
                targetField,
                relationship,
                lineNo,
                lineRelationSeq,
                lineContent,
                confidence
        );
    }
}
