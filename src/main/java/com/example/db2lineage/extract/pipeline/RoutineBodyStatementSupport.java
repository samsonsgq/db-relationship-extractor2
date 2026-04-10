package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.parse.ParsedStatementResult;
import com.example.db2lineage.parse.SqlStatementParser;
import com.example.db2lineage.parse.StatementSlice;

import java.util.ArrayList;
import java.util.List;

final class RoutineBodyStatementSupport {

    private static final List<StatementExtractor> NESTED_EXTRACTORS = List.of(
            new SelectStatementExtractor(),
            new InsertStatementExtractor(),
            new UpdateStatementExtractor(),
            new DeleteStatementExtractor(),
            new MergeStatementExtractor(),
            new TruncateStatementExtractor(),
            new ExecuteStatementExtractor(),
            new UnknownStatementExtractor(),
            new ProceduralFallbackStatementExtractor()
    );

    private RoutineBodyStatementSupport() {
    }

    static void extractNestedStatements(ParsedStatementResult routineStatement,
                                        ExtractionContext context,
                                        RowCollector collector) {
        List<StatementSlice> nestedSlices = splitRoutineBody(routineStatement.slice());
        SqlStatementParser parser = new SqlStatementParser();
        for (StatementSlice nestedSlice : nestedSlices) {
            ParsedStatementResult nestedParsed = parser.parse(nestedSlice);
            for (StatementExtractor extractor : NESTED_EXTRACTORS) {
                if (extractor.supports(nestedParsed)) {
                    extractor.extract(nestedParsed, context, collector);
                    break;
                }
            }
        }
    }

    private static List<StatementSlice> splitRoutineBody(StatementSlice routineSlice) {
        List<String> rawLines = routineSlice.rawLines();
        List<StatementSlice> result = new ArrayList<>();

        int bodyStart = -1;
        int bodyEndExclusive = rawLines.size();
        for (int i = 0; i < rawLines.size(); i++) {
            String upper = rawLines.get(i).trim().toUpperCase();
            if (bodyStart < 0 && upper.startsWith("BEGIN")) {
                bodyStart = i + 1;
                continue;
            }
            if (bodyStart >= 0 && (upper.equals("END") || upper.equals("END;"))) {
                bodyEndExclusive = i;
                break;
            }
        }

        if (bodyStart < 0 || bodyStart >= bodyEndExclusive) {
            return List.of();
        }

        List<String> current = new ArrayList<>();
        int statementStartLine = -1;
        int nestedOrdinal = 0;
        ScanState state = new ScanState();

        for (int i = bodyStart; i < bodyEndExclusive; i++) {
            String line = rawLines.get(i);
            int lineNo = routineSlice.startLine() + i;
            if (statementStartLine < 0) {
                statementStartLine = lineNo;
            }
            current.add(line);

            if (endsStatement(line, state)) {
                String statementText = joinAndTrimTrailingDelimiter(current);
                if (!statementText.isBlank()) {
                    result.add(new StatementSlice(
                            routineSlice.sourceFile(),
                            routineSlice.sourceCategory(),
                            statementText,
                            statementStartLine,
                            lineNo,
                            List.copyOf(current),
                            routineSlice.ordinalWithinFile() * 1_000 + nestedOrdinal++
                    ));
                }
                current = new ArrayList<>();
                statementStartLine = -1;
                state = new ScanState();
                continue;
            }

            state = updateState(line, state);
        }

        if (!current.isEmpty()) {
            String statementText = String.join("\n", current).trim();
            if (!statementText.isBlank()) {
                int endLine = routineSlice.startLine() + bodyEndExclusive - 1;
                result.add(new StatementSlice(
                        routineSlice.sourceFile(),
                        routineSlice.sourceCategory(),
                        statementText,
                        statementStartLine,
                        endLine,
                        List.copyOf(current),
                        routineSlice.ordinalWithinFile() * 1_000 + nestedOrdinal
                ));
            }
        }

        return List.copyOf(result);
    }

    private static boolean endsStatement(String line, ScanState state) {
        if (state.insideBlockComment || state.insideSingleQuote) {
            return false;
        }
        String trimmed = line.trim();
        return trimmed.endsWith(";");
    }

    private static String joinAndTrimTrailingDelimiter(List<String> lines) {
        if (lines.isEmpty()) {
            return "";
        }
        List<String> copy = new ArrayList<>(lines);
        int last = copy.size() - 1;
        String tail = copy.get(last);
        int idx = tail.lastIndexOf(';');
        if (idx >= 0) {
            copy.set(last, tail.substring(0, idx));
        }
        return String.join("\n", copy).trim();
    }

    private static ScanState updateState(String line, ScanState prior) {
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
