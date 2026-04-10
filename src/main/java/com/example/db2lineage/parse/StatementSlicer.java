package com.example.db2lineage.parse;

import java.util.ArrayList;
import java.util.List;

public final class StatementSlicer {

    public List<StatementSlice> slice(SqlSourceFile sourceFile) {
        List<StatementSlice> slices = new ArrayList<>();
        List<String> currentLines = new ArrayList<>();

        int startLine = -1;
        int ordinalWithinFile = 0;
        boolean hasNonBlankContent = false;
        ScanState scanState = new ScanState();

        for (int idx = 0; idx < sourceFile.rawLines().size(); idx++) {
            String line = sourceFile.rawLines().get(idx);
            int lineNo = idx + 1;

            if (isDelimiterLine(line, scanState)) {
                if (startLine != -1 && hasNonBlankContent) {
                    slices.add(buildSlice(sourceFile, currentLines, startLine, lineNo - 1, ordinalWithinFile++));
                }

                currentLines = new ArrayList<>();
                startLine = -1;
                hasNonBlankContent = false;
                continue;
            }

            if (startLine == -1) {
                startLine = lineNo;
            }
            currentLines.add(line);
            if (!line.isBlank()) {
                hasNonBlankContent = true;
            }

            scanState = updateState(line, scanState);
        }

        if (startLine != -1 && hasNonBlankContent) {
            slices.add(buildSlice(sourceFile, currentLines, startLine, sourceFile.getLineCount(), ordinalWithinFile));
        }

        return List.copyOf(slices);
    }

    private StatementSlice buildSlice(
            SqlSourceFile sourceFile,
            List<String> lines,
            int startLine,
            int endLine,
            int ordinalWithinFile
    ) {
        return new StatementSlice(
                sourceFile,
                sourceFile.sourceCategory(),
                String.join("\n", lines),
                startLine,
                endLine,
                lines,
                ordinalWithinFile
        );
    }

    private boolean isDelimiterLine(String line, ScanState scanState) {
        return !scanState.insideBlockComment && !scanState.insideSingleQuote && line.trim().equals("@");
    }

    private ScanState updateState(String line, ScanState prior) {
        boolean insideBlockComment = prior.insideBlockComment;
        boolean insideSingleQuote = prior.insideSingleQuote;

        int i = 0;
        while (i < line.length()) {
            char c = line.charAt(i);
            char next = (i + 1 < line.length()) ? line.charAt(i + 1) : '\0';

            if (!insideSingleQuote && !insideBlockComment && c == '-' && next == '-') {
                break;
            }

            if (!insideSingleQuote && !insideBlockComment && c == '/' && next == '*') {
                insideBlockComment = true;
                i += 2;
                continue;
            }

            if (!insideSingleQuote && insideBlockComment && c == '*' && next == '/') {
                insideBlockComment = false;
                i += 2;
                continue;
            }

            if (!insideBlockComment && c == '\'') {
                if (insideSingleQuote && next == '\'') {
                    i += 2;
                    continue;
                }
                insideSingleQuote = !insideSingleQuote;
            }

            i++;
        }

        return new ScanState(insideBlockComment, insideSingleQuote);
    }

    private record ScanState(boolean insideBlockComment, boolean insideSingleQuote) {
        private ScanState() {
            this(false, false);
        }
    }
}
