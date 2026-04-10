package com.example.db2lineage.model;

import java.util.Objects;

public record RowDraft(
        SourceObjectType sourceObjectType,
        String sourceObject,
        String sourceField,
        TargetObjectType targetObjectType,
        String targetObject,
        String targetField,
        RelationshipType relationship,
        int lineNo,
        String lineContent,
        ConfidenceLevel confidence,
        int statementOrder,
        int naturalOrderOnLine
) {

    public RowDraft {
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
        lineContent = requireNonNull(lineContent, "lineContent");
        if (lineContent.indexOf('\n') >= 0 || lineContent.indexOf('\r') >= 0 || lineContent.indexOf('\t') >= 0) {
            throw new IllegalArgumentException("lineContent must be a single raw source line without tabs");
        }
        confidence = requireNonNull(confidence, "confidence");
        if (statementOrder < 0) {
            throw new IllegalArgumentException("statementOrder must be >= 0");
        }
        if (naturalOrderOnLine < 0) {
            throw new IllegalArgumentException("naturalOrderOnLine must be >= 0");
        }
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
