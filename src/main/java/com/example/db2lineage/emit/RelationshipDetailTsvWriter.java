package com.example.db2lineage.emit;

import com.example.db2lineage.model.RelationshipRow;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class RelationshipDetailTsvWriter {

    public static final String HEADER = RelationshipRow.TSV_HEADER;
    public static final String OUTPUT_FILE_NAME = "relationship_detail.tsv";

    public Path writeToOutputDir(Path outputDir, List<RelationshipRow> rows) throws IOException {
        return write(outputDir.resolve(OUTPUT_FILE_NAME), rows);
    }

    public Path write(Path outputFile, List<RelationshipRow> rows) throws IOException {
        if (outputFile.getParent() != null) {
            Files.createDirectories(outputFile.getParent());
        }

        List<RelationshipRow> sortedRows = new ArrayList<>(rows);
        sortedRows.sort(RelationshipRow.STABLE_OUTPUT_COMPARATOR);

        List<String> lines = new ArrayList<>();
        lines.add(HEADER);
        for (RelationshipRow row : sortedRows) {
            lines.add(row.toTsvLine());
        }

        Files.write(outputFile, lines, StandardCharsets.UTF_8);
        return outputFile;
    }
}
