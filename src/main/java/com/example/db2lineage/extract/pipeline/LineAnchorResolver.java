package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.parse.StatementSlice;

import java.util.List;
import java.util.Locale;

final class LineAnchorResolver {

    private LineAnchorResolver() {
    }

    static LineAnchor statementStart(StatementSlice slice, int fallbackOrderOnLine) {
        int lineNo = slice.startLine();
        return new LineAnchor(lineNo, rawLine(slice, lineNo), Math.max(0, fallbackOrderOnLine), false);
    }

    static LineAnchor token(StatementSlice slice, String token, int fallbackOrderOnLine) {
        String needle = searchable(token);
        if (needle.isBlank()) {
            return statementStart(slice, fallbackOrderOnLine);
        }

        List<String> lines = slice.sourceFile().rawLines();
        String upperNeedle = needle.toUpperCase(Locale.ROOT);
        for (int lineNo = slice.startLine(); lineNo <= slice.endLine(); lineNo++) {
            String line = rawLine(slice, lineNo);
            int idx = line.toUpperCase(Locale.ROOT).indexOf(upperNeedle);
            if (idx >= 0) {
                return new LineAnchor(lineNo, line, idx, true);
            }
        }

        // Conservative fallback when we cannot prove exact token anchoring:
        // use statement-start raw source line rather than a guessed nearby line.
        return statementStart(slice, fallbackOrderOnLine);
    }

    private static String rawLine(StatementSlice slice, int oneBasedLineNo) {
        if (oneBasedLineNo >= 1 && oneBasedLineNo <= slice.sourceFile().getLineCount()) {
            return slice.sourceFile().getRawLine(oneBasedLineNo);
        }
        return "";
    }

    private static String searchable(String token) {
        if (token == null) {
            return "";
        }
        if (token.startsWith("CONSTANT:")) {
            return token.substring("CONSTANT:".length());
        }
        if (token.startsWith("FUNCTION:")) {
            return token.substring("FUNCTION:".length());
        }
        return token;
    }

    record LineAnchor(int lineNo, String lineContent, int orderOnLine, boolean exact) {
    }
}
