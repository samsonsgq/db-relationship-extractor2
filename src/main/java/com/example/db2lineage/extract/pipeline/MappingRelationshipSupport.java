package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.ConfidenceLevel;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.RowDraft;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import com.example.db2lineage.parse.StatementSlice;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NextValExpression;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

final class MappingRelationshipSupport {
    private static final Map<String, String> SPECIAL_REGISTER_CANONICAL = Map.ofEntries(
            Map.entry("CURRENT DATE", "CURRENT DATE"),
            Map.entry("CURRENT_TIME", "CURRENT TIME"),
            Map.entry("CURRENT TIME", "CURRENT TIME"),
            Map.entry("CURRENT_TIMESTAMP", "CURRENT TIMESTAMP"),
            Map.entry("CURRENT TIMESTAMP", "CURRENT TIMESTAMP"),
            Map.entry("CURRENT_USER", "CURRENT USER"),
            Map.entry("CURRENT USER", "CURRENT USER"),
            Map.entry("SESSION_USER", "SESSION USER"),
            Map.entry("SESSION USER", "SESSION USER"),
            Map.entry("SYSTEM_USER", "SYSTEM USER"),
            Map.entry("SYSTEM USER", "SYSTEM USER"),
            Map.entry("USER", "USER")
    );

    private MappingRelationshipSupport() {
    }

    static void addTargetColumnRow(RelationshipType relationship,
                                   String targetObject,
                                   TargetObjectType targetType,
                                   String targetField,
                                   ParsedStatementResult parsedStatement,
                                   ExtractionContext context,
                                   RowCollector collector,
                                   int naturalOrder) {
        TokenPosition targetPosition = locateToken(parsedStatement.slice(), targetField, naturalOrder);
        collector.addDraft(new RowDraft(
                ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                "",
                targetType,
                ObjectRelationshipSupport.normalizeObjectName(targetObject),
                targetField,
                relationship,
                targetPosition.lineNo(),
                targetPosition.lineContent(),
                ConfidenceLevel.PARSER,
                ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                targetPosition.orderOnLine()
        ));
    }

    static void addConciseMappingRows(RelationshipType mappingRelationship,
                                      String targetObject,
                                      TargetObjectType targetType,
                                      String targetField,
                                      Expression expression,
                                      ParsedStatementResult parsedStatement,
                                      ExtractionContext context,
                                      RowCollector collector,
                                      int naturalOrder) {
        addConciseMappingRows(
                mappingRelationship,
                targetObject,
                targetType,
                targetField,
                expression,
                parsedStatement,
                context,
                collector,
                naturalOrder,
                -1,
                -1
        );
    }

    static void addConciseMappingRows(RelationshipType mappingRelationship,
                                      String targetObject,
                                      TargetObjectType targetType,
                                      String targetField,
                                      Expression expression,
                                      ParsedStatementResult parsedStatement,
                                      ExtractionContext context,
                                      RowCollector collector,
                                      int naturalOrder,
                                      int startLine,
                                      int endLine) {
        List<ExpressionTokenSupport.TokenUse> tokens = conciseMappingTokens(expression, parsedStatement.slice(), mappingRelationship, startLine, endLine);
        for (ExpressionTokenSupport.TokenUse tokenUse : tokens) {
            RelationshipType effectiveRelationship = strongestDirectMappingRelationship(mappingRelationship, expression, tokenUse.token());
            String sourceToken = normalizeMappingSourceToken(effectiveRelationship, tokenUse.token(), parsedStatement);
            collector.addDraft(new RowDraft(
                    ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                    ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                    sourceToken,
                    targetType,
                    ObjectRelationshipSupport.normalizeObjectName(targetObject),
                    targetField,
                    effectiveRelationship,
                    tokenUse.lineNo(),
                    tokenUse.lineContent(),
                    ConfidenceLevel.PARSER,
                    ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                    naturalOrder + tokenUse.orderOnLine()
            ));
        }
        if (mappingRelationship != RelationshipType.INSERT_SELECT_MAP) {
            addFunctionExpressionRows(targetObject, targetType, targetField, expression, parsedStatement, context, collector, naturalOrder + 10_000);
        }
    }

    private static RelationshipType strongestDirectMappingRelationship(RelationshipType mappingRelationship,
                                                                      Expression expression,
                                                                      String sourceToken) {
        if (mappingRelationship != RelationshipType.INSERT_SELECT_MAP
                && mappingRelationship != RelationshipType.UPDATE_SET_MAP
                && mappingRelationship != RelationshipType.MERGE_SET_MAP
                && mappingRelationship != RelationshipType.MERGE_INSERT_MAP
                && mappingRelationship != RelationshipType.VARIABLE_SET_MAP) {
            return mappingRelationship;
        }
        if (isDirectSequenceExpression(expression) && sourceToken != null && sourceToken.startsWith("SEQUENCE:")) {
            return RelationshipType.SEQUENCE_VALUE_MAP;
        }
        if (isDirectSpecialRegisterExpression(expression) && sourceToken != null && sourceToken.startsWith("CONSTANT:")) {
            return RelationshipType.SPECIAL_REGISTER_MAP;
        }
        return mappingRelationship;
    }

    private static boolean isDirectSequenceExpression(Expression expression) {
        return expression instanceof NextValExpression;
    }

    private static boolean isDirectSpecialRegisterExpression(Expression expression) {
        return directSpecialRegisterToken(expression) != null;
    }

    static List<ExpressionTokenSupport.TokenUse> conciseMappingTokens(Expression expression, StatementSlice slice) {
        return conciseMappingTokens(expression, slice, null, -1, -1);
    }

    static List<ExpressionTokenSupport.TokenUse> conciseMappingTokens(Expression expression, StatementSlice slice, RelationshipType mappingRelationship) {
        return conciseMappingTokens(expression, slice, mappingRelationship, -1, -1);
    }

    static List<ExpressionTokenSupport.TokenUse> conciseMappingTokens(Expression expression,
                                                                      StatementSlice slice,
                                                                      RelationshipType mappingRelationship,
                                                                      int startLine,
                                                                      int endLine) {
        if (expression == null) {
            return List.of();
        }
        String directSpecialRegister = directSpecialRegisterToken(expression);
        if (directSpecialRegister != null) {
            TokenPosition anchor = (startLine > 0 && endLine >= startLine)
                    ? locateTokenInRange(slice, directSpecialRegister, startLine, endLine, 0)
                    : locateToken(slice, directSpecialRegister, 0);
            return List.of(new ExpressionTokenSupport.TokenUse(
                    "CONSTANT:" + directSpecialRegister,
                    anchor.lineNo(),
                    anchor.lineContent(),
                    anchor.orderOnLine()
            ));
        }
        if (expression instanceof CaseExpression caseExpression) {
            return fromCaseExpression(caseExpression, slice, startLine, endLine);
        }
        if (mappingRelationship != RelationshipType.INSERT_SELECT_MAP && containsFunction(expression)) {
            // Conservative default for non-insert mapping families.
            return List.of();
        }
        List<ExpressionTokenSupport.TokenUse> usageTokens =
                (startLine > 0 && endLine >= startLine)
                        ? ExpressionTokenSupport.collect(expression, slice, startLine, endLine)
                        : ExpressionTokenSupport.collect(expression, slice);
        List<ExpressionTokenSupport.TokenUse> filtered = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (ExpressionTokenSupport.TokenUse token : usageTokens) {
            if (token.token().startsWith("FUNCTION:")) {
                continue;
            }
            String key = token.token() + "|" + token.lineNo() + "|" + token.orderOnLine();
            if (seen.add(key)) {
                filtered.add(token);
            }
        }
        return List.copyOf(filtered);
    }

    private static String directSpecialRegisterToken(Expression expression) {
        if (expression instanceof TimeKeyExpression timeKeyExpression) {
            return normalizeSpecialRegisterName(timeKeyExpression.getStringValue());
        }
        if (expression instanceof Column column) {
            String name = column.getFullyQualifiedName();
            if (name == null || name.contains(".")) {
                return null;
            }
            return normalizeSpecialRegisterName(name);
        }
        if (expression instanceof Function function) {
            if (function.getParameters() != null
                    && function.getParameters().getExpressions() != null
                    && !function.getParameters().getExpressions().isEmpty()) {
                return null;
            }
            return normalizeSpecialRegisterName(function.getName());
        }
        return null;
    }

    private static String normalizeSpecialRegisterName(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String normalized = raw.toUpperCase(Locale.ROOT).trim().replaceAll("\\s+", " ");
        String underscoreForm = normalized.replace(' ', '_');
        if (SPECIAL_REGISTER_CANONICAL.containsKey(underscoreForm)) {
            return SPECIAL_REGISTER_CANONICAL.get(underscoreForm);
        }
        return SPECIAL_REGISTER_CANONICAL.get(normalized);
    }

    private static List<ExpressionTokenSupport.TokenUse> fromCaseExpression(CaseExpression caseExpression,
                                                                            StatementSlice slice,
                                                                            int startLine,
                                                                            int endLine) {
        List<ExpressionTokenSupport.TokenUse> tokens = new ArrayList<>();
        int boundedStartLine = startLine > 0 ? Math.max(slice.startLine(), startLine) : slice.startLine();
        int boundedEndLine = endLine > 0 ? Math.min(slice.endLine(), endLine) : slice.endLine();
        if (boundedEndLine < boundedStartLine) {
            return List.of();
        }
        int searchStartLine = boundedStartLine;
        if (caseExpression.getWhenClauses() != null) {
            for (WhenClause whenClause : caseExpression.getWhenClauses()) {
                Expression whenExpression = whenClause.getWhenExpression();
                int whenLine = findCaseBranchAnchorLine(slice, "WHEN", whenExpression, searchStartLine, boundedEndLine);

                Expression thenExpression = whenClause.getThenExpression();
                int thenLine = findCaseBranchAnchorLine(slice, "THEN", thenExpression, whenLine, boundedEndLine);

                if (thenLine > whenLine) {
                    tokens.addAll(conciseMappingTokensWithinRange(whenExpression, slice, whenLine, thenLine));
                }
                tokens.addAll(conciseMappingTokensWithinRange(whenExpression, slice, whenLine, boundedEndLine));

                Expression thenExpression = whenClause.getThenExpression();
                int thenLine = findCaseBranchAnchorLine(slice, "THEN", thenExpression, whenLine, boundedEndLine);
                tokens.addAll(conciseMappingTokensWithinRange(thenExpression, slice, thenLine, boundedEndLine));
                searchStartLine = Math.min(boundedEndLine, Math.max(whenLine, thenLine) + 1);
            }
        }
        if (caseExpression.getElseExpression() != null) {
            int elseLine = findCaseBranchAnchorLine(slice, "ELSE", caseExpression.getElseExpression(), searchStartLine, boundedEndLine);
            tokens.addAll(conciseMappingTokensWithinRange(caseExpression.getElseExpression(), slice, elseLine, boundedEndLine));
        }
        return dedupe(tokens);
    }

    private static List<ExpressionTokenSupport.TokenUse> conciseMappingTokensWithinRange(Expression expression,
                                                                                         StatementSlice slice,
                                                                                         int startLine,
                                                                                         int endLine) {
        if (expression == null) {
            return List.of();
        }
        List<ExpressionTokenSupport.TokenUse> usageTokens = ExpressionTokenSupport.collect(expression, slice, startLine, endLine);
        List<ExpressionTokenSupport.TokenUse> filtered = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (ExpressionTokenSupport.TokenUse token : usageTokens) {
            if (token.token().startsWith("FUNCTION:")) {
                continue;
            }
            String key = token.token() + "|" + token.lineNo() + "|" + token.orderOnLine();
            if (seen.add(key)) {
                filtered.add(token);
            }
        }
        return List.copyOf(filtered);
    }

    private static int findCaseBranchAnchorLine(StatementSlice slice,
                                                String branchKeyword,
                                                Expression branchExpression,
                                                int fallbackStartLine,
                                                int endLine) {
        if (branchExpression == null) {
            return Math.max(slice.startLine(), fallbackStartLine);
        }
        String exprText = branchExpression.toString().toUpperCase(Locale.ROOT);
        int startLine = Math.max(slice.startLine(), fallbackStartLine);
        int boundedEndLine = Math.min(slice.endLine(), endLine);
        for (int line = startLine; line <= boundedEndLine; line++) {
            String raw = slice.sourceFile().getRawLine(line).toUpperCase(Locale.ROOT);
            int branchIdx = raw.indexOf(branchKeyword);
            int exprIdx = branchIdx >= 0
                    ? raw.indexOf(exprText, branchIdx + branchKeyword.length())
                    : -1;
            if (branchIdx >= 0 && exprIdx > branchIdx) {
                return line;
            }
        }
        for (int line = startLine; line <= boundedEndLine; line++) {
            String raw = slice.sourceFile().getRawLine(line).toUpperCase(Locale.ROOT);
            if (raw.contains(branchKeyword)) {
                return line;
            }
        }
        return startLine;
    }

    private static List<ExpressionTokenSupport.TokenUse> dedupe(List<ExpressionTokenSupport.TokenUse> tokens) {
        List<ExpressionTokenSupport.TokenUse> deduped = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (ExpressionTokenSupport.TokenUse token : tokens) {
            String key = token.token() + "|" + token.lineNo() + "|" + token.orderOnLine();
            if (seen.add(key)) {
                deduped.add(token);
            }
        }
        return List.copyOf(deduped);
    }

    private static void addFunctionExpressionRows(String targetObject,
                                                  TargetObjectType targetType,
                                                  String targetField,
                                                  Expression expression,
                                                  ParsedStatementResult parsedStatement,
                                                  ExtractionContext context,
                                                  RowCollector collector,
                                                  int naturalOrder) {
        if (!(expression instanceof Function function)) {
            return;
        }
        if (function.getName() == null || function.getName().isBlank()) {
            return;
        }
        String functionName = function.getName();
        TokenPosition position = locateToken(parsedStatement.slice(), functionName, naturalOrder);
        collector.addDraft(new RowDraft(
                ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                functionName.toUpperCase(Locale.ROOT),
                targetType,
                ObjectRelationshipSupport.normalizeObjectName(targetObject),
                targetField,
                RelationshipType.FUNCTION_EXPR_MAP,
                position.lineNo(),
                position.lineContent(),
                ConfidenceLevel.PARSER,
                ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                naturalOrder
        ));
    }

    static TokenPosition locateToken(StatementSlice slice, String token, int fallbackOrder) {
        LineAnchorResolver.LineAnchor anchor = LineAnchorResolver.token(slice, token, fallbackOrder);
        return new TokenPosition(anchor.lineNo(), anchor.lineContent(), anchor.orderOnLine());
    }

    static TokenPosition locateTokenInRange(StatementSlice slice,
                                            String token,
                                            int startLine,
                                            int endLine,
                                            int fallbackOrder) {
        String needle = token == null ? "" : token;
        if (needle.startsWith("CONSTANT:")) {
            needle = needle.substring("CONSTANT:".length());
        } else if (needle.startsWith("FUNCTION:")) {
            needle = needle.substring("FUNCTION:".length());
        }
        String upperNeedle = needle.toUpperCase(Locale.ROOT);
        int boundedStart = Math.max(slice.startLine(), startLine);
        int boundedEnd = Math.min(slice.endLine(), endLine);
        if (!upperNeedle.isBlank()) {
            for (int lineNo = boundedStart; lineNo <= boundedEnd; lineNo++) {
                String line = slice.sourceFile().getRawLine(lineNo);
                int idx = line.toUpperCase(Locale.ROOT).indexOf(upperNeedle);
                if (idx >= 0) {
                    return new TokenPosition(lineNo, line, idx);
                }
            }
        }
        return locateToken(slice, token, fallbackOrder);
    }

    private static String normalizeMappingSourceToken(RelationshipType relationship,
                                                      String token,
                                                      ParsedStatementResult parsedStatement) {
        if (token == null || token.isBlank() || token.startsWith("CONSTANT:") || token.startsWith("FUNCTION:")) {
            return token == null ? "" : token;
        }
        if (token.startsWith("SEQUENCE:")) {
            return token;
        }
        if (relationship != RelationshipType.INSERT_SELECT_MAP
                && relationship != RelationshipType.UPDATE_SET_MAP
                || ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice())
                != com.example.db2lineage.model.SourceObjectType.PROCEDURE) {
            return token;
        }
        int dot = token.lastIndexOf('.');
        if (dot > 0 && dot < token.length() - 1) {
            return token.substring(dot + 1);
        }
        return token;
    }

    private static boolean containsFunction(Expression expression) {
        final boolean[] found = {false};
        expression.accept(new net.sf.jsqlparser.expression.ExpressionVisitorAdapter<Void>() {
            @Override
            public <S> Void visit(Function function, S context) {
                found[0] = true;
                return null;
            }
        }, null);
        return found[0];
    }

    record TokenPosition(int lineNo, String lineContent, int orderOnLine) {
    }
}
