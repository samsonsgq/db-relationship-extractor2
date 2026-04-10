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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ProceduralFallbackStatementExtractor implements StatementExtractor {
    private static final Pattern SET_ASSIGNMENT = Pattern.compile("(?i)^\\s*SET\\s+([A-Z0-9_.$]+)\\s*=\\s*(.+?)\\s*;?\\s*$");

    @Override
    public boolean supports(ParsedStatementResult parsedStatement) {
        return parsedStatement.parseFailed()
                || parsedStatement.unsupported()
                || parsedStatement.statement().isEmpty();
    }

    @Override
    public void extract(ParsedStatementResult parsedStatement, ExtractionContext context, RowCollector collector) {
        Matcher setMatcher = SET_ASSIGNMENT.matcher(parsedStatement.slice().statementText());
        if (setMatcher.find()) {
            String variable = setMatcher.group(1);
            String expressionText = setMatcher.group(2);
            try {
                Expression expression = CCJSqlParserUtil.parseExpression(expressionText);
                MappingRelationshipSupport.addConciseMappingRows(
                        RelationshipType.VARIABLE_SET_MAP,
                        ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                        TargetObjectType.VARIABLE,
                        variable,
                        expression,
                        parsedStatement,
                        context,
                        collector,
                        0
                );
                return;
            } catch (JSQLParserException ignored) {
                // fall through to generic fallback row
            }
        }

        String dynamicToken = ObjectRelationshipSupport.extractDynamicSqlSourceToken(parsedStatement.slice().statementText());
        if (!dynamicToken.isBlank()) {
            collector.addDraft(new RowDraft(
                    ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                    ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                    dynamicToken,
                    TargetObjectType.UNKNOWN,
                    ObjectRelationshipSupport.UNKNOWN_DYNAMIC_SQL,
                    "",
                    RelationshipType.DYNAMIC_SQL_EXEC,
                    parsedStatement.slice().startLine(),
                    ObjectRelationshipSupport.firstLine(parsedStatement.slice()),
                    ConfidenceLevel.DYNAMIC_LOW_CONFIDENCE,
                    ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                    0
            ));
            return;
        }

        collector.addDraft(ObjectRelationshipSupport.objectLevelDraft(
                context,
                parsedStatement,
                RelationshipType.UNKNOWN,
                TargetObjectType.UNKNOWN,
                ObjectRelationshipSupport.UNKNOWN_UNSUPPORTED_STATEMENT,
                0
        ));
    }
}
