package com.example.db2lineage.diff;

import com.example.db2lineage.emit.RelationshipDetailTsvWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationshipDetailTsvDifferTest {

    @TempDir
    Path tempDir;

    @Test
    void reportsMissingAndUnexpectedRows() throws Exception {
        Path expected = tempDir.resolve("expected.tsv");
        Path actual = tempDir.resolve("actual.tsv");

        Files.writeString(expected, RelationshipDetailTsvWriter.HEADER + "\nA\nB\n");
        Files.writeString(actual, RelationshipDetailTsvWriter.HEADER + "\nA\nC\n");

        TsvDiffResult result = new RelationshipDetailTsvDiffer().diff(expected, actual);

        assertFalse(result.identical());
        assertEquals(1, result.missingCount());
        assertEquals(1, result.unexpectedCount());
        assertTrue(result.missingRows().contains("B"));
        assertTrue(result.unexpectedRows().contains("C"));
    }
}
