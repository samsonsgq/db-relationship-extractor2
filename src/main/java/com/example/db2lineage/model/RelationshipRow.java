package com.example.db2lineage.model;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public record RelationshipRow(
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

    public static final List<String> TSV_HEADER_COLUMNS = List.of(
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

    public static final String TSV_HEADER = String.join("\t", TSV_HEADER_COLUMNS);

    public static final Comparator<RelationshipRow> STABLE_OUTPUT_COMPARATOR =
            Comparator.comparing(RelationshipRow::sourceObjectType)
                    .thenComparing(RelationshipRow::sourceObject)
                    .thenComparingInt(RelationshipRow::lineNo)
                    .thenComparingInt(RelationshipRow::lineRelationSeq)
                    .thenComparing(RelationshipRow::relationship)
                    .thenComparing(RelationshipRow::targetObjectType)
                    .thenComparing(RelationshipRow::targetObject)
                    .thenComparing(RelationshipRow::targetField)
                    .thenComparing(RelationshipRow::sourceField)
                    .thenComparing(RelationshipRow::lineContent)
                    .thenComparing(RelationshipRow::confidence);

    public RelationshipRow {
        sourceObjectType = requireNonNull(sourceObjectType, "sourceObjectType");
        sourceObject = requireNonBlank(sourceObject, "sourceObject");
        sourceField = sanitizeTsvValue(sourceField, "sourceField");
        targetObjectType = requireNonNull(targetObjectType, "targetObjectType");
        targetObject = requireNonBlank(targetObject, "targetObject");
        targetField = sanitizeTsvValue(targetField, "targetField");
        relationship = requireNonNull(relationship, "relationship");
        if (lineNo < 1) {
            throw new IllegalArgumentException("lineNo must be >= 1");
        }
        if (lineRelationSeq < 0) {
            throw new IllegalArgumentException("lineRelationSeq must be >= 0");
        }
        lineContent = requireNonNull(lineContent, "lineContent");
        if (lineContent.indexOf('\n') >= 0 || lineContent.indexOf('\r') >= 0 || lineContent.indexOf('\t') >= 0) {
            throw new IllegalArgumentException("lineContent must be a single raw source line without tabs");
        }
        confidence = requireNonNull(confidence, "confidence");
    }

    public String toTsvLine() {
        return String.join("\t",
                sourceObjectType.name(),
                sourceObject,
                sourceField,
                targetObjectType.name(),
                targetObject,
                targetField,
                relationship.name(),
                Integer.toString(lineNo),
                Integer.toString(lineRelationSeq),
                lineContent,
                confidence.name()
        );
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return sanitizeTsvValue(value, name);
    }

    private static <T> T requireNonNull(T value, String name) {
        return Objects.requireNonNull(value, name + " must not be null");
    }

    private static String sanitizeTsvValue(String value, String name) {
        String sanitized = value == null ? "" : value;
        if (sanitized.indexOf('\n') >= 0 || sanitized.indexOf('\r') >= 0 || sanitized.indexOf('\t') >= 0) {
            throw new IllegalArgumentException(name + " must not contain tabs or newlines");
        }
        return sanitized;
    }
}
