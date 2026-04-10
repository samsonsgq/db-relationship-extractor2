package com.example.db2lineage.parse;

import net.sf.jsqlparser.statement.Statement;

import java.util.List;
import java.util.Optional;

public record ParsedStatementResult(
        StatementSlice slice,
        Optional<Statement> statement,
        List<ParseIssue> parseIssues,
        boolean unsupported,
        boolean parseFailed
) {
    public ParsedStatementResult {
        parseIssues = List.copyOf(parseIssues);
    }

    public static ParsedStatementResult parsed(StatementSlice slice, Statement statement) {
        return new ParsedStatementResult(slice, Optional.of(statement), List.of(), false, false);
    }

    public static ParsedStatementResult unsupported(StatementSlice slice, List<ParseIssue> parseIssues) {
        return new ParsedStatementResult(slice, Optional.empty(), parseIssues, true, false);
    }

    public static ParsedStatementResult parseFailed(StatementSlice slice, List<ParseIssue> parseIssues) {
        return new ParsedStatementResult(slice, Optional.empty(), parseIssues, false, true);
    }
}
