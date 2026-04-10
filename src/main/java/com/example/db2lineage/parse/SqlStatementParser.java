package com.example.db2lineage.parse;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.util.ArrayList;
import java.util.List;

public final class SqlStatementParser {

    public ParsedStatementResult parse(StatementSlice slice) {
        try {
            Statement statement = CCJSqlParserUtil.parse(slice.statementText());
            return ParsedStatementResult.parsed(slice, statement);
        } catch (JSQLParserException e) {
            List<ParseIssue> issues = new ArrayList<>();
            issues.add(new ParseIssue("JSQLPARSER_ERROR", e.getMessage()));

            if (isLikelyUnsupportedDb2ProceduralSyntax(slice.statementText())) {
                issues.add(new ParseIssue(
                        "UNSUPPORTED_DB2_PROCEDURAL_SYNTAX",
                        "Reserved for DB2-specific fallback parsing in a later phase."
                ));
                return ParsedStatementResult.unsupported(slice, issues);
            }

            return ParsedStatementResult.parseFailed(slice, issues);
        }
    }

    public List<ParsedStatementResult> parseAll(List<StatementSlice> slices) {
        List<ParsedStatementResult> results = new ArrayList<>();
        for (StatementSlice slice : slices) {
            results.add(parse(slice));
        }
        return List.copyOf(results);
    }

    private boolean isLikelyUnsupportedDb2ProceduralSyntax(String statementText) {
        String normalized = statementText.stripLeading().toUpperCase();
        return normalized.startsWith("GET DIAGNOSTICS")
                || normalized.startsWith("DECLARE CONTINUE HANDLER")
                || normalized.startsWith("DECLARE EXIT HANDLER")
                || normalized.startsWith("SIGNAL ")
                || normalized.startsWith("RESIGNAL ");
    }
}
