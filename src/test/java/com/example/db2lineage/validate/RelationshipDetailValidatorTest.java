package com.example.db2lineage.validate;

import com.example.db2lineage.emit.RelationshipDetailTsvWriter;
import com.example.db2lineage.parse.SqlSourceCategory;
import com.example.db2lineage.parse.SqlSourceFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationshipDetailValidatorTest {

    @TempDir
    Path tempDir;

    @Test
    void detectsHeaderAndLineContentIssues() throws Exception {
        Path tsv = tempDir.resolve("relationship_detail.tsv");
        Files.writeString(tsv, "bad_header\n"
                + "PROCEDURE\tPROC_A\t\tTABLE\tTAB_A\t\tSELECT_TABLE\t1\t0\tSELECT * FROM TAB_A\tPARSER\n");

        SqlSourceFile sourceFile = new SqlSourceFile(
                SqlSourceCategory.SP_DIR,
                tempDir.resolve("PROC_A.sql"),
                Path.of("PROC_A.sql"),
                "SELECT 1",
                List.of("SELECT 1")
        );

        ValidationReport report = new RelationshipDetailValidator().validate(tsv, List.of(sourceFile));

        assertFalse(report.valid());
        assertTrue(report.issues().stream().anyMatch(issue -> issue.code().equals("HEADER_MISMATCH")));
        assertTrue(report.issues().stream().anyMatch(issue -> issue.code().equals("LINE_CONTENT_MISMATCH")));
    }

    @Test
    void acceptsMinimalValidFile() throws Exception {
        Path tsv = tempDir.resolve("relationship_detail.tsv");
        String line = "PROCEDURE\tPROC_A\t\tTABLE\tTAB_A\t\tSELECT_TABLE\t1\t0\tSELECT * FROM TAB_A\tPARSER";
        Files.writeString(tsv, RelationshipDetailTsvWriter.HEADER + "\n" + line + "\n");

        SqlSourceFile sourceFile = new SqlSourceFile(
                SqlSourceCategory.SP_DIR,
                tempDir.resolve("PROC_A.sql"),
                Path.of("PROC_A.sql"),
                "SELECT * FROM TAB_A",
                List.of("SELECT * FROM TAB_A")
        );

        ValidationReport report = new RelationshipDetailValidator().validate(tsv, List.of(sourceFile));
        assertTrue(report.valid());
    }
}
