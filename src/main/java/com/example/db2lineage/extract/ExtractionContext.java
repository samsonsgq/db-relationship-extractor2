package com.example.db2lineage.extract;

import com.example.db2lineage.parse.SqlSourceFile;
import com.example.db2lineage.resolve.SchemaMetadataService;

import java.util.List;

public record ExtractionContext(List<SqlSourceFile> sourceFiles, SchemaMetadataService schemaMetadataService) {
}
