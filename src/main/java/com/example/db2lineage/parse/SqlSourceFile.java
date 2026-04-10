package com.example.db2lineage.parse;

import com.example.db2lineage.model.SourceObjectType;

import java.nio.file.Path;

public record SqlSourceFile(
        SourceObjectType sourceObjectType,
        String sourceObject,
        Path path
) {
}
