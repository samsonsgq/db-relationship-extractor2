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
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

final class MappingRelationshipSupport {
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
        if (expression instanceof NextValExpression) {
            addDirectStrongestMappingRow(
                    RelationshipType.SEQUENCE_VALUE_MAP,
                    targetObject,
                    targetType,
                    targetField,
                    expression,
                    parsedStatement,
                    context,
                    collector,
                    naturalOrder
            );
            return;
        }
        if (isSpecialRegisterExpression(expression)) {
            addDirectStrongestMappingRow(
                    RelationshipType.SPECIAL_REGISTER_MAP,
                    targetObject,
                    targetType,
                    targetField,
                    expression,
                    parsedStatement,
                    context,
                    collector,
                    naturalOrder
            );
            return;
        }
        List<ExpressionTokenSupport.TokenUse> tokens = conciseMappingTokens(expression, parsedStatement.slice(), mappingRelationship, startLine, endLine);
        for (ExpressionTokenSupport.TokenUse tokenUse : tokens) {
            String sourceToken = normalizeMappingSourceToken(mappingRelationship, tokenUse.token(), parsedStatement);
            collector.addDraft(new RowDraft(
                    ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                    ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                    sourceToken,
                    targetType,
                    ObjectRelationshipSupport.normalizeObjectName(targetObject),
                    targetField,
                    mappingRelationship,
                    tokenUse.lineNo(),
                    tokenUse.lineContent(),
                    ConfidenceLevel.PARSER,
                    ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                    naturalOrder + tokenUse.orderOnLine()
            ));
        }
        addFunctionExpressionRows(targetObject, targetType, targetField, expression, parsedStatement, context, collector, naturalOrder + 10_000);
    }

    private static void addDirectStrongestMappingRow(RelationshipType strongestRelationship,
                                                     String targetObject,
                                                     TargetObjectType targetType,
                                                     String targetField,
                                                     Expression expression,
                                                     ParsedStatementResult parsedStatement,
                                                     ExtractionContext context,
                                                     RowCollector collector,
                                                     int naturalOrder) {
        List<ExpressionTokenSupport.TokenUse> tokens = ExpressionTokenSupport.collect(expression, parsedStatement.slice());
        if (tokens.isEmpty()) {
            return;
        }
        ExpressionTokenSupport.TokenUse tokenUse = tokens.get(0);
        String sourceToken = normalizeMappingSourceToken(strongestRelationship, tokenUse.token(), parsedStatement);
        collector.addDraft(new RowDraft(
                ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                sourceToken,
                targetType,
                ObjectRelationshipSupport.normalizeObjectName(targetObject),
                targetField,
                strongestRelationship,
                tokenUse.lineNo(),
                tokenUse.lineContent(),
                ConfidenceLevel.PARSER,
                ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                naturalOrder + tokenUse.orderOnLine()
        ));
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
                Expression thenExpression = whenClause.getThenExpression();
                int thenLine = findCaseBranchAnchorLine(slice, "THEN", thenExpression, searchStartLine, boundedEndLine);
                tokens.addAll(conciseMappingTokensWithinRange(thenExpression, slice, thenLine, boundedEndLine));
                searchStartLine = Math.min(boundedEndLine, thenLine + 1);
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
        if (expression == null) {
            return;
        }
        if (!(expression instanceof Function function)
                || function.getName() == null
                || function.getName().isBlank()) {
            return;
        }
        TokenPosition position = locateToken(parsedStatement.slice(), function.getName(), naturalOrder);
        collector.addDraft(new RowDraft(
                ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                function.getName().toUpperCase(Locale.ROOT),
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

    private static boolean isSpecialRegisterExpression(Expression expression) {
        if (expression == null) {
            return false;
        }
        String normalized = expression.toString().trim().toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");
        return "CURRENT TIMESTAMP".equals(normalized)
                || "CURRENT DATE".equals(normalized)
                || "CURRENT TIME".equals(normalized)
                || "CURRENT USER".equals(normalized)
                || "USER".equals(normalized)
                || "SESSION_USER".equals(normalized);
    }

    record TokenPosition(int lineNo, String lineContent, int orderOnLine) {
    }
}
