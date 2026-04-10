package com.example.db2lineage.parse;

public record StatementSlice(
        String sql,
        int startLine,
        int endLine
) {
}
