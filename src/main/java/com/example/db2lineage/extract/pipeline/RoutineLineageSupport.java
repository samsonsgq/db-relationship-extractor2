package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.ConfidenceLevel;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.RowDraft;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import com.example.db2lineage.parse.StatementSlice;
import com.example.db2lineage.resolve.SchemaMetadataService;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class RoutineLineageSupport {
    private static final Pattern CALL_PATTERN = Pattern.compile("(?i)\\bCALL\\s+([A-Z0-9_.$\\\"]+)\\s*\\((.*)\\)\\s*;?");
    private static final Pattern FUNCTION_CALL_ASSIGNMENT = Pattern.compile("(?i)^\\s*(?:SET\\s+)?([A-Z0-9_.$]+)\\s*=\\s*([A-Z0-9_.$\\\"]+)\\s*\\((.*)\\)\\s*;?\\s*$");
    private static final Pattern DECLARE_CURSOR_PATTERN = Pattern.compile("(?i)^\\s*DECLARE\\s+([A-Z0-9_.$]+)\\s+CURSOR\\b.*$");
    private static final Pattern OPEN_CURSOR_PATTERN = Pattern.compile("(?i)^\\s*OPEN\\s+([A-Z0-9_.$]+)\\s*;?\\s*$");
    private static final Pattern CLOSE_CURSOR_PATTERN = Pattern.compile("(?i)^\\s*CLOSE\\s+([A-Z0-9_.$]+)\\s*;?\\s*$");
    private static final Pattern FETCH_CURSOR_PATTERN = Pattern.compile("(?i)^\\s*FETCH\\s+(?:NEXT\\s+FROM\\s+|FROM\\s+)?([A-Z0-9_.$]+)\\s+INTO\\s+(.+?)\\s*;?\\s*$");
    private static final Pattern GET_DIAGNOSTICS_PATTERN = Pattern.compile("(?i)^\\s*GET\\s+DIAGNOSTICS\\s+([A-Z0-9_.$]+)\\s*=\\s*([A-Z0-9_.$]+)\\s*;?\\s*$");
    private static final Pattern SPECIAL_REGISTER_ASSIGNMENT = Pattern.compile("(?i)^\\s*(?:SET\\s+)?([A-Z0-9_.$]+)\\s*=\\s*(CURRENT\\s+DATE|CURRENT\\s+TIMESTAMP|CURRENT\\s+USER|USER|SQLSTATE|SQLCODE)\\s*;?\\s*$");
    private static final Pattern HANDLER_PATTERN = Pattern.compile("(?i)^\\s*DECLARE\\s+(?:CONTINUE|EXIT)\\s+HANDLER\\s+FOR\\s+([A-Z0-9_.$,\\s]+?)\\s+(?:SET\\s+)?([A-Z0-9_.$]+)\\s*=\\s*(.+?)\\s*;?\\s*$");
    private static final Pattern EXECUTE_IMMEDIATE_PATTERN = Pattern.compile("(?i)^\\s*EXECUTE\\s+IMMEDIATE\\s+(.+?)\\s*;?\\s*$");
    private static final Pattern IF_CONDITION_PATTERN = Pattern.compile("(?i)^\\s*IF\\s+(.+?)\\s+THEN\\b.*$");
    private static final Pattern WHILE_CONDITION_PATTERN = Pattern.compile("(?i)^\\s*WHILE\\s+(.+?)\\s+DO\\b.*$");
    private static final Pattern UNTIL_CONDITION_PATTERN = Pattern.compile("(?i)^\\s*UNTIL\\s+(.+?)\\s*$");
    private static final Pattern CASE_WHEN_CONDITION_PATTERN = Pattern.compile("(?i)^\\s*WHEN\\s+(.+?)\\s+THEN\\b.*$");

    private RoutineLineageSupport() {
    }

    static boolean extractLine(String line,
                               int lineNo,
                               ParsedStatementResult parsedStatement,
                               ExtractionContext context,
                               RowCollector collector,
                               int baseNaturalOrder) {
        if (extractCall(line, lineNo, parsedStatement, context, collector, baseNaturalOrder)) {
            return true;
        }
        if (extractFunctionAssignment(line, lineNo, parsedStatement, context, collector, baseNaturalOrder)) {
            return true;
        }
        if (extractCursorRows(line, lineNo, parsedStatement, context, collector, baseNaturalOrder)) {
            return true;
        }
        if (extractDiagnostics(line, lineNo, parsedStatement, context, collector, baseNaturalOrder)) {
            return true;
        }
        if (extractSpecialRegister(line, lineNo, parsedStatement, context, collector, baseNaturalOrder)) {
            return true;
        }
        if (extractHandler(line, lineNo, parsedStatement, context, collector, baseNaturalOrder)) {
            return true;
        }
        if (extractControlFlowCondition(line, lineNo, parsedStatement, context, collector, baseNaturalOrder)) {
            return true;
        }
        return extractDynamicSql(line, lineNo, parsedStatement, context, collector, baseNaturalOrder);
    }

    private static boolean extractControlFlowCondition(String line,
                                                       int lineNo,
                                                       ParsedStatementResult parsedStatement,
                                                       ExtractionContext context,
                                                       RowCollector collector,
                                                       int baseNaturalOrder) {
        String condition = firstMatchedGroup(line, IF_CONDITION_PATTERN, WHILE_CONDITION_PATTERN, UNTIL_CONDITION_PATTERN, CASE_WHEN_CONDITION_PATTERN);
        if (condition == null || condition.isBlank()) {
            return false;
        }

        Expression expression;
        try {
            expression = CCJSqlParserUtil.parseCondExpression(condition.trim());
        } catch (JSQLParserException ignored) {
            return false;
        }

        TargetObjectType targetType = ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice())
                == com.example.db2lineage.model.SourceObjectType.FUNCTION
                ? TargetObjectType.FUNCTION
                : TargetObjectType.PROCEDURE;
        String targetObject = ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice());

        List<ExpressionTokenSupport.TokenUse> tokens = ExpressionTokenSupport.collect(expression, parsedStatement.slice());
        if (tokens.isEmpty()) {
            return false;
        }

        for (ExpressionTokenSupport.TokenUse tokenUse : tokens) {
            collector.addDraft(lineDraft(
                    parsedStatement,
                    context,
                    tokenUse.token(),
                    targetType,
                    targetObject,
                    "",
                    RelationshipType.CONTROL_FLOW_CONDITION,
                    tokenUse.lineNo(),
                    tokenUse.lineContent(),
                    baseNaturalOrder + tokenUse.orderOnLine()
            ));
        }
        return true;
    }

    @SafeVarargs
    private static String firstMatchedGroup(String line, Pattern... patterns) {
        for (Pattern pattern : patterns) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                return matcher.group(1);
            }
        }
        return null;
    }

    private static boolean extractCall(String line, int lineNo, ParsedStatementResult parsedStatement,
                                       ExtractionContext context, RowCollector collector, int baseNaturalOrder) {
        Matcher call = CALL_PATTERN.matcher(line);
        if (!call.find()) {
            return false;
        }
        String callable = call.group(1).trim();
        List<String> args = splitArgs(call.group(2));
        collector.addDraft(lineDraft(parsedStatement, context, "", TargetObjectType.PROCEDURE, callable, "",
                RelationshipType.CALL_PROCEDURE, lineNo, line, baseNaturalOrder));
        addParameterRows(parsedStatement, context, collector, lineNo, line, callable, args, RelationshipType.CALL_PARAM_MAP, baseNaturalOrder + 10);
        return true;
    }

    private static boolean extractFunctionAssignment(String line, int lineNo, ParsedStatementResult parsedStatement,
                                                     ExtractionContext context, RowCollector collector, int baseNaturalOrder) {
        Matcher assignment = FUNCTION_CALL_ASSIGNMENT.matcher(line);
        if (!assignment.find()) {
            return false;
        }
        String targetVariable = assignment.group(1).trim();
        String functionName = assignment.group(2).trim();
        List<String> args = splitArgs(assignment.group(3));
        addParameterRows(parsedStatement, context, collector, lineNo, line, functionName, args, RelationshipType.FUNCTION_PARAM_MAP, baseNaturalOrder);
        collector.addDraft(lineDraft(parsedStatement, context, functionName, TargetObjectType.VARIABLE, targetVariable, targetVariable,
                RelationshipType.FUNCTION_EXPR_MAP, lineNo, line, baseNaturalOrder + 100));
        return true;
    }

    private static void addParameterRows(ParsedStatementResult parsedStatement,
                                         ExtractionContext context,
                                         RowCollector collector,
                                         int lineNo,
                                         String line,
                                         String callable,
                                         List<String> args,
                                         RelationshipType relationshipType,
                                         int baseNaturalOrder) {
        SchemaMetadataService.CallableSignature signature = context.schemaMetadataService()
                .resolveCallableSignature(callable)
                .orElse(null);
        List<String> names = signature == null ? List.of() : signature.argumentNames();
        boolean useNamed = !names.isEmpty();
        for (int i = 0; i < args.size(); i++) {
            String arg = args.get(i);
            String targetField = useNamed && i < names.size() ? names.get(i) : "$" + (i + 1);
            String sourceToken = normalizeSourceToken(arg);
            collector.addDraft(lineDraft(parsedStatement, context, sourceToken, relationshipType == RelationshipType.FUNCTION_PARAM_MAP ? TargetObjectType.FUNCTION : TargetObjectType.PROCEDURE,
                    callable, targetField, relationshipType, lineNo, line, baseNaturalOrder + i));
        }
    }

    private static boolean extractCursorRows(String line, int lineNo, ParsedStatementResult parsedStatement,
                                             ExtractionContext context, RowCollector collector, int baseNaturalOrder) {
        Matcher declare = DECLARE_CURSOR_PATTERN.matcher(line);
        if (declare.find()) {
            String cursor = declare.group(1).trim();
            collector.addDraft(lineDraft(parsedStatement, context, "", TargetObjectType.CURSOR, cursor, "",
                    RelationshipType.CURSOR_DEFINE, lineNo, line, baseNaturalOrder));
            return true;
        }
        Matcher open = OPEN_CURSOR_PATTERN.matcher(line);
        if (open.find()) {
            String cursor = open.group(1).trim();
            collector.addDraft(lineDraft(parsedStatement, context, "", TargetObjectType.CURSOR, cursor, "",
                    RelationshipType.CURSOR_READ, lineNo, line, baseNaturalOrder));
            return true;
        }
        Matcher close = CLOSE_CURSOR_PATTERN.matcher(line);
        if (close.find()) {
            String cursor = close.group(1).trim();
            collector.addDraft(lineDraft(parsedStatement, context, "", TargetObjectType.CURSOR, cursor, "",
                    RelationshipType.CURSOR_READ, lineNo, line, baseNaturalOrder));
            return true;
        }
        Matcher fetch = FETCH_CURSOR_PATTERN.matcher(line);
        if (!fetch.find()) {
            return false;
        }
        String cursor = fetch.group(1).trim();
        collector.addDraft(lineDraft(parsedStatement, context, "", TargetObjectType.CURSOR, cursor, "",
                RelationshipType.CURSOR_READ, lineNo, line, baseNaturalOrder));
        List<String> targets = splitArgs(fetch.group(2));
        for (int i = 0; i < targets.size(); i++) {
            collector.addDraft(lineDraft(parsedStatement, context, cursor, TargetObjectType.VARIABLE, targets.get(i).trim(), targets.get(i).trim(),
                    RelationshipType.CURSOR_FETCH_MAP, lineNo, line, baseNaturalOrder + 10 + i));
        }
        return true;
    }

    private static boolean extractDiagnostics(String line, int lineNo, ParsedStatementResult parsedStatement,
                                              ExtractionContext context, RowCollector collector, int baseNaturalOrder) {
        Matcher diagnostics = GET_DIAGNOSTICS_PATTERN.matcher(line);
        if (!diagnostics.find()) {
            return false;
        }
        collector.addDraft(lineDraft(parsedStatement, context,
                "CONSTANT:" + diagnostics.group(2).trim().toUpperCase(Locale.ROOT),
                TargetObjectType.VARIABLE,
                diagnostics.group(1).trim(),
                diagnostics.group(1).trim(),
                RelationshipType.DIAGNOSTICS_FETCH_MAP,
                lineNo,
                line,
                baseNaturalOrder));
        return true;
    }

    private static boolean extractSpecialRegister(String line, int lineNo, ParsedStatementResult parsedStatement,
                                                  ExtractionContext context, RowCollector collector, int baseNaturalOrder) {
        Matcher special = SPECIAL_REGISTER_ASSIGNMENT.matcher(line);
        if (!special.find()) {
            return false;
        }
        collector.addDraft(lineDraft(parsedStatement, context,
                "CONSTANT:" + special.group(2).trim().toUpperCase(Locale.ROOT),
                TargetObjectType.VARIABLE,
                special.group(1).trim(),
                special.group(1).trim(),
                RelationshipType.SPECIAL_REGISTER_MAP,
                lineNo,
                line,
                baseNaturalOrder));
        return true;
    }

    private static boolean extractHandler(String line, int lineNo, ParsedStatementResult parsedStatement,
                                          ExtractionContext context, RowCollector collector, int baseNaturalOrder) {
        Matcher handler = HANDLER_PATTERN.matcher(line);
        if (!handler.find()) {
            return false;
        }
        collector.addDraft(lineDraft(parsedStatement, context,
                "CONSTANT:" + handler.group(1).trim().toUpperCase(Locale.ROOT),
                TargetObjectType.VARIABLE,
                handler.group(2).trim(),
                handler.group(2).trim(),
                RelationshipType.EXCEPTION_HANDLER_MAP,
                lineNo,
                line,
                baseNaturalOrder));
        return true;
    }

    private static boolean extractDynamicSql(String line, int lineNo, ParsedStatementResult parsedStatement,
                                             ExtractionContext context, RowCollector collector, int baseNaturalOrder) {
        Matcher dynamic = EXECUTE_IMMEDIATE_PATTERN.matcher(line);
        if (!dynamic.find()) {
            return false;
        }
        collector.addDraft(lineDraft(parsedStatement, context,
                dynamic.group(1).trim(),
                TargetObjectType.UNKNOWN,
                ObjectRelationshipSupport.UNKNOWN_DYNAMIC_SQL,
                "",
                RelationshipType.DYNAMIC_SQL_EXEC,
                lineNo,
                line,
                baseNaturalOrder));
        return true;
    }

    static boolean extractSetAssignment(String statementText,
                                        String targetObject,
                                        ParsedStatementResult parsedStatement,
                                        ExtractionContext context,
                                        RowCollector collector,
                                        int naturalOrder) {
        Pattern setAssignment = Pattern.compile("(?i)^\\s*SET\\s+([A-Z0-9_.$]+)\\s*=\\s*(.+?)\\s*;?\\s*$");
        Matcher setMatcher = setAssignment.matcher(statementText);
        if (!setMatcher.find()) {
            return false;
        }
        String variable = setMatcher.group(1);
        String expressionText = setMatcher.group(2);
        try {
            Expression expression = CCJSqlParserUtil.parseExpression(expressionText);
            MappingRelationshipSupport.addConciseMappingRows(
                    RelationshipType.VARIABLE_SET_MAP,
                    targetObject,
                    TargetObjectType.VARIABLE,
                    variable,
                    expression,
                    parsedStatement,
                    context,
                    collector,
                    naturalOrder
            );
            return true;
        } catch (JSQLParserException ignored) {
            return false;
        }
    }

    private static RowDraft lineDraft(ParsedStatementResult parsedStatement,
                                      ExtractionContext context,
                                      String sourceField,
                                      TargetObjectType targetType,
                                      String targetObject,
                                      String targetField,
                                      RelationshipType relationshipType,
                                      int lineNo,
                                      String line,
                                      int naturalOrder) {
        return new RowDraft(
                ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                sourceField,
                targetType,
                ObjectRelationshipSupport.normalizeObjectName(targetObject),
                targetField,
                relationshipType,
                lineNo,
                line,
                ConfidenceLevel.PARSER,
                ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                naturalOrder
        );
    }

    private static List<String> splitArgs(String rawArgs) {
        if (rawArgs == null || rawArgs.isBlank()) {
            return List.of();
        }
        List<String> args = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        for (int i = 0; i < rawArgs.length(); i++) {
            char c = rawArgs.charAt(i);
            if (c == '(') {
                depth++;
                current.append(c);
            } else if (c == ')') {
                depth = Math.max(0, depth - 1);
                current.append(c);
            } else if (c == ',' && depth == 0) {
                args.add(current.toString().trim());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        if (!current.isEmpty()) {
            args.add(current.toString().trim());
        }
        return List.copyOf(args);
    }

    private static String normalizeSourceToken(String arg) {
        if (arg == null) {
            return "";
        }
        String trimmed = arg.trim();
        String upper = trimmed.toUpperCase(Locale.ROOT);
        if (upper.equals("CURRENT DATE") || upper.equals("CURRENT TIMESTAMP") || upper.equals("CURRENT USER") || upper.equals("USER")) {
            return "CONSTANT:" + upper;
        }
        if (trimmed.matches("^'.*'$") || trimmed.matches("^[0-9]+(?:\\.[0-9]+)?$")) {
            return "CONSTANT:" + trimmed;
        }
        return trimmed;
    }
}
