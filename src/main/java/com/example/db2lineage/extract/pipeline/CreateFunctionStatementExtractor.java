package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.ConfidenceLevel;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.RowDraft;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.function.CreateFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class CreateFunctionStatementExtractor implements StatementExtractor {
    private static final Pattern RETURN_EXPRESSION = Pattern.compile("(?i)\\bRETURN\\b\\s*(.+?)(?:;|$)");
    private static final Pattern RETURNS_TABLE_COLUMNS = Pattern.compile("(?is)\\bRETURNS\\s+TABLE\\s*\\((.*?)\\)");
    private static final Pattern PIPE_PATTERN = Pattern.compile("(?i)\\bPIPE\\s*\\((.*)\\)\\s*;?\\s*$");
    @Override
    public boolean supports(ParsedStatementResult parsedStatement) {
        return parsedStatement.statement().filter(CreateFunction.class::isInstance).isPresent();
    }

    @Override
    public void extract(ParsedStatementResult parsedStatement, ExtractionContext context, RowCollector collector) {
        CreateFunction createFunction = (CreateFunction) parsedStatement.statement().orElseThrow();
        String name = extractName(createFunction.getFunctionDeclarationParts());
        collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                context,
                parsedStatement,
                RelationshipType.CREATE_FUNCTION,
                TargetObjectType.FUNCTION,
                name,
                0
        ));

        int naturalOrder = 1;
        List<String> returnColumns = extractReturnTableColumns(parsedStatement.slice().statementText());
        for (int i = 0; i < parsedStatement.slice().rawLines().size(); i++) {
            String line = parsedStatement.slice().rawLines().get(i);
            int lineNo = parsedStatement.slice().startLine() + i;
            RoutineLineageSupport.extractLine(line, lineNo, parsedStatement, context, collector, 1_000 + (i * 100));
            if (!returnColumns.isEmpty()) {
                naturalOrder = extractPipeMappings(line, returnColumns, name, parsedStatement, context, collector, naturalOrder);
            }
            Matcher matcher = RETURN_EXPRESSION.matcher(line);
            if (!matcher.find()) {
                continue;
            }
            String exprText = matcher.group(1).trim();
            if (exprText.isEmpty()) {
                continue;
            }
            try {
                Expression expression = CCJSqlParserUtil.parseExpression(exprText);
                for (ExpressionTokenSupport.TokenUse tokenUse : ExpressionTokenSupport.collect(expression, parsedStatement.slice(), lineNo, lineNo)) {
                    collector.addDraft(new RowDraft(
                            ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                            ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                            tokenUse.token(),
                            TargetObjectType.UNKNOWN,
                            ObjectRelationshipSupport.UNKNOWN_UNRESOLVED_OBJECT,
                            "",
                            RelationshipType.RETURN_VALUE,
                            tokenUse.lineNo(),
                            tokenUse.lineContent(),
                            ConfidenceLevel.PARSER,
                            ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                            naturalOrder + tokenUse.orderOnLine()
                    ));
                }
                naturalOrder++;
            } catch (JSQLParserException ignored) {
                // narrow fallback: skip unparseable return expression tokenization
            }
        }
        RoutineBodyStatementSupport.extractNestedStatements(parsedStatement, context, collector);
    }

    private String extractName(List<String> declarationParts) {
        if (declarationParts == null || declarationParts.isEmpty()) {
            return ObjectRelationshipSupport.UNKNOWN_UNRESOLVED_OBJECT;
        }
        return declarationParts.get(0);
    }

    private int extractPipeMappings(String line,
                                    List<String> returnColumns,
                                    String functionName,
                                    ParsedStatementResult parsedStatement,
                                    ExtractionContext context,
                                    RowCollector collector,
                                    int naturalOrder) {
        Matcher pipe = PIPE_PATTERN.matcher(line);
        if (!pipe.find()) {
            return naturalOrder;
        }
        List<String> args = splitTopLevel(pipe.group(1));
        int slots = Math.min(returnColumns.size(), args.size());
        for (int i = 0; i < slots; i++) {
            String arg = args.get(i).trim();
            if (arg.isBlank()) {
                continue;
            }
            try {
                Expression expression = CCJSqlParserUtil.parseExpression(arg);
                MappingRelationshipSupport.addConciseMappingRows(
                        RelationshipType.TABLE_FUNCTION_RETURN_MAP,
                        functionName,
                        TargetObjectType.FUNCTION,
                        returnColumns.get(i),
                        expression,
                        parsedStatement,
                        context,
                        collector,
                        naturalOrder++
                );
            } catch (JSQLParserException ignored) {
                // narrow fallback: skip unparseable PIPE expression tokenization
            }
        }
        return naturalOrder;
    }

    private List<String> extractReturnTableColumns(String statementText) {
        Matcher matcher = RETURNS_TABLE_COLUMNS.matcher(statementText == null ? "" : statementText);
        if (!matcher.find()) {
            return List.of();
        }
        List<String> columns = new ArrayList<>();
        for (String segment : splitTopLevel(matcher.group(1))) {
            String trimmed = segment.trim();
            if (trimmed.isBlank()) {
                continue;
            }
            String[] parts = trimmed.split("\\s+");
            if (parts.length > 0) {
                columns.add(parts[0].toUpperCase(Locale.ROOT));
            }
        }
        return List.copyOf(columns);
    }

    private List<String> splitTopLevel(String text) {
        if (text == null || text.isBlank()) {
            return List.of();
        }
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth = Math.max(0, depth - 1);
            } else if (c == ',' && depth == 0) {
                values.add(current.toString());
                current.setLength(0);
                continue;
            }
            current.append(c);
        }
        if (!current.isEmpty()) {
            values.add(current.toString());
        }
        return List.copyOf(values);
    }
}
