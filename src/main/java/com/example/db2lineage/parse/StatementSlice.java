package com.example.db2lineage.parse;

import java.util.List;

public record StatementSlice(
        SqlSourceFile sourceFile,
        SqlSourceCategory sourceCategory,
        String statementText,
        int startLine,
        int endLine,
        List<String> rawLines,
        int ordinalWithinFile
) {
    public StatementSlice {
        rawLines = List.copyOf(rawLines);
    }
}
