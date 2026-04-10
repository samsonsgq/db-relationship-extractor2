package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.model.ConfidenceLevel;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.RowDraft;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import com.example.db2lineage.parse.StatementSlice;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

final class ExpressionTokenSupport {

    private static final Set<String> SPECIAL_VALUES = Set.of(
            "CURRENT DATE", "CURRENT_DATE",
            "CURRENT TIME", "CURRENT_TIME",
            "CURRENT TIMESTAMP", "CURRENT_TIMESTAMP",
            "CURRENT USER", "CURRENT_USER",
            "SESSION_USER", "SYSTEM_USER", "USER"
    );

    private ExpressionTokenSupport() {
    }

    static List<TokenUse> collect(Expression expression, StatementSlice slice) {
        return collect(expression, slice, slice.startLine(), slice.endLine());
    }

    static List<TokenUse> collect(Expression expression, StatementSlice slice, int startLine, int endLine) {
        if (expression == null) {
            return List.of();
        }
        List<String> rawTokens = new ArrayList<>();
        expression.accept(new ExpressionVisitorAdapter<Void>() {
            @Override
            public <S> Void visit(Column column, S context) {
                String value = column.getFullyQualifiedName();
                if (value == null || value.isBlank()) {
                    return null;
                }
                String normalizedUpper = value.toUpperCase(Locale.ROOT).replace('_', ' ').trim();
                if (!value.contains(".") && SPECIAL_VALUES.contains(normalizedUpper)) {
                    rawTokens.add("CONSTANT:" + value);
                } else {
                    rawTokens.add(value);
                }
                return null;
            }

            @Override
            public <S> Void visit(Function function, S context) {
                String name = function.getName();
                if (name != null && !name.isBlank()) {
                    rawTokens.add("FUNCTION:" + name);
                }
                return super.visit(function, context);
            }

            @Override
            public <S> Void visit(StringValue stringValue, S context) {
                rawTokens.add("CONSTANT:" + stringValue);
                return null;
            }

            @Override
            public <S> Void visit(LongValue longValue, S context) {
                rawTokens.add("CONSTANT:" + longValue.getStringValue());
                return null;
            }

            @Override
            public <S> Void visit(DoubleValue doubleValue, S context) {
                rawTokens.add("CONSTANT:" + doubleValue.getValue());
                return null;
            }

            @Override
            public <S> Void visit(HexValue hexValue, S context) {
                rawTokens.add("CONSTANT:" + hexValue.getValue());
                return null;
            }

            @Override
            public <S> Void visit(NullValue nullValue, S context) {
                rawTokens.add("CONSTANT:NULL");
                return null;
            }

            @Override
            public <S> Void visit(DateTimeLiteralExpression literal, S context) {
                rawTokens.add("CONSTANT:" + literal);
                return null;
            }

            @Override
            public <S> Void visit(TimeKeyExpression timeKeyExpression, S context) {
                rawTokens.add("CONSTANT:" + timeKeyExpression.getStringValue());
                return null;
            }

            @Override
            public <S> Void visit(JdbcParameter jdbcParameter, S context) {
                rawTokens.add("?");
                return null;
            }

            @Override
            public <S> Void visit(JdbcNamedParameter jdbcNamedParameter, S context) {
                rawTokens.add(jdbcNamedParameter.toString());
                return null;
            }

            @Override
            public <S> Void visit(UserVariable userVariable, S context) {
                rawTokens.add(userVariable.toString());
                return null;
            }

            @Override
            public <S> Void visit(NumericBind numericBind, S context) {
                rawTokens.add(numericBind.toString());
                return null;
            }
        }, null);

        List<TokenUse> uses = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        int fallbackOrder = 0;
        int boundedStart = Math.max(slice.startLine(), startLine);
        int boundedEnd = Math.min(slice.endLine(), endLine);
        if (boundedStart > boundedEnd) {
            boundedStart = slice.startLine();
            boundedEnd = slice.endLine();
        }
        for (String token : rawTokens) {
            LineAnchorResolver.LineAnchor p = tokenInRange(slice, token, fallbackOrder, boundedStart, boundedEnd);
            String dedupeKey = token + "|" + p.lineNo() + "|" + p.orderOnLine();
            if (seen.add(dedupeKey)) {
                uses.add(new TokenUse(token, p.lineNo(), p.lineContent(), p.orderOnLine()));
            }
            fallbackOrder++;
        }
        return List.copyOf(uses);
    }

    private static LineAnchorResolver.LineAnchor tokenInRange(StatementSlice slice,
                                                              String token,
                                                              int fallbackOrderOnLine,
                                                              int startLine,
                                                              int endLine) {
        String needle = searchableToken(token);
        if (needle.isBlank()) {
            return LineAnchorResolver.statementStart(slice, fallbackOrderOnLine);
        }
        String upperNeedle = needle.toUpperCase(Locale.ROOT);
        for (int lineNo = startLine; lineNo <= endLine; lineNo++) {
            String line = slice.sourceFile().getRawLine(lineNo);
            int idx = line.toUpperCase(Locale.ROOT).indexOf(upperNeedle);
            if (idx >= 0) {
                return new LineAnchorResolver.LineAnchor(lineNo, line, idx, true);
            }
        }
        return new LineAnchorResolver.LineAnchor(startLine, slice.sourceFile().getRawLine(startLine), Math.max(0, fallbackOrderOnLine), false);
    }

    private static String searchableToken(String token) {
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

    static void addExpressionRows(RelationshipType relationship,
                                  Expression expression,
                                  ParsedStatementResult parsedStatement,
                                  ExtractionContext context,
                                  com.example.db2lineage.extract.RowCollector collector,
                                  int baseOrder) {
        for (TokenUse tokenUse : collect(expression, parsedStatement.slice())) {
            collector.addDraft(new RowDraft(
                    ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                    ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                    tokenUse.token(),
                    TargetObjectType.UNKNOWN,
                    ObjectRelationshipSupport.UNKNOWN_UNRESOLVED_OBJECT,
                    "",
                    relationship,
                    tokenUse.lineNo(),
                    tokenUse.lineContent(),
                    ConfidenceLevel.PARSER,
                    ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                    baseOrder + tokenUse.orderOnLine()
            ));
        }
    }

    record TokenUse(String token, int lineNo, String lineContent, int orderOnLine) {
    }
}
