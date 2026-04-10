package com.example.db2lineage.emit;

import com.example.db2lineage.model.RelationshipRow;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class RelationshipDetailTsvWriter {

    private static final String HEADER = String.join("\t",
            "source_object_type",
            "source_object",
            "source_field",
            "target_object_type",
            "target_object",
            "target_field",
            "relationship",
            "line_no",
            "line_relation_seq",
            "line_content",
            "confidence"
    );

    public void write(Path outputFile, List<RelationshipRow> rows) throws IOException {
        Files.createDirectories(outputFile.getParent());

        List<String> lines = new ArrayList<>();
        lines.add(HEADER);
        for (RelationshipRow row : rows) {
            lines.add(toLine(row));
        }

        Files.write(outputFile, lines, StandardCharsets.UTF_8);
    }

    private String toLine(RelationshipRow row) {
        return String.join("\t",
                row.sourceObjectType().name(),
                row.sourceObject(),
                row.sourceField(),
                row.targetObjectType().name(),
                row.targetObject(),
                row.targetField(),
                row.relationship().name(),
                Integer.toString(row.lineNo()),
                Integer.toString(row.lineRelationSeq()),
                row.lineContent(),
                row.confidence().name()
        );
    }
}
