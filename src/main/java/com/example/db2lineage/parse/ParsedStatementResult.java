package com.example.db2lineage.parse;

import net.sf.jsqlparser.statement.Statement;

import java.util.Optional;

public record ParsedStatementResult(
        StatementSlice slice,
        Optional<Statement> statement,
        Optional<String> parseError
) {
}
