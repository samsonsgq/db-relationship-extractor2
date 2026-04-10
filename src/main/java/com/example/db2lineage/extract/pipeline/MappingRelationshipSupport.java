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
        List<ExpressionTokenSupport.TokenUse> tokens = conciseMappingTokens(expression, parsedStatement.slice());
        for (ExpressionTokenSupport.TokenUse tokenUse : tokens) {
            collector.addDraft(new RowDraft(
                    ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                    ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                    tokenUse.token(),
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

    static List<ExpressionTokenSupport.TokenUse> conciseMappingTokens(Expression expression, StatementSlice slice) {
        if (expression == null) {
            return List.of();
        }
        if (expression instanceof CaseExpression caseExpression) {
            return fromCaseExpression(caseExpression, slice);
        }
        if (containsFunction(expression)) {
            // Conservative rule: avoid mapping function argument tokens directly into assignment targets.
            // Keep mapping truthful by relying on FUNCTION_EXPR_MAP and dedicated parameter mappings.
            return List.of();
        }
        List<ExpressionTokenSupport.TokenUse> usageTokens = ExpressionTokenSupport.collect(expression, slice);
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

    private static List<ExpressionTokenSupport.TokenUse> fromCaseExpression(CaseExpression caseExpression, StatementSlice slice) {
        List<ExpressionTokenSupport.TokenUse> tokens = new ArrayList<>();
        if (caseExpression.getWhenClauses() != null) {
            for (WhenClause whenClause : caseExpression.getWhenClauses()) {
                tokens.addAll(conciseMappingTokens(whenClause.getThenExpression(), slice));
            }
        }
        if (caseExpression.getElseExpression() != null) {
            tokens.addAll(conciseMappingTokens(caseExpression.getElseExpression(), slice));
        }
        return dedupe(tokens);
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
        Set<String> functions = new LinkedHashSet<>();
        expression.accept(new net.sf.jsqlparser.expression.ExpressionVisitorAdapter<Void>() {
            @Override
            public <S> Void visit(Function function, S context1) {
                if (function.getName() != null && !function.getName().isBlank()) {
                    functions.add(function.getName());
                }
                return super.visit(function, context1);
            }
        }, null);
        int i = 0;
        for (String function : functions) {
            TokenPosition position = locateToken(parsedStatement.slice(), function, naturalOrder + i);
            collector.addDraft(new RowDraft(
                    ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                    ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                    function.toUpperCase(Locale.ROOT),
                    targetType,
                    ObjectRelationshipSupport.normalizeObjectName(targetObject),
                    targetField,
                    RelationshipType.FUNCTION_EXPR_MAP,
                    position.lineNo(),
                    position.lineContent(),
                    ConfidenceLevel.PARSER,
                    ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                    naturalOrder + i
            ));
            i++;
        }
    }

    static TokenPosition locateToken(StatementSlice slice, String token, int fallbackOrder) {
        LineAnchorResolver.LineAnchor anchor = LineAnchorResolver.token(slice, token, fallbackOrder);
        return new TokenPosition(anchor.lineNo(), anchor.lineContent(), anchor.orderOnLine());
    }

    record TokenPosition(int lineNo, String lineContent, int orderOnLine) {
    }
}
