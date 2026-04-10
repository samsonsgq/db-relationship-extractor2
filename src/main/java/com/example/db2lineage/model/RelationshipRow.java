package com.example.db2lineage.model;

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
}
