package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.function.CreateFunction;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class CreateFunctionStatementExtractor implements StatementExtractor {
    private static final Pattern RETURN_EXPRESSION = Pattern.compile("(?i)\\bRETURN\\b\\s*(.+?)(?:;|$)");
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
        for (int i = 0; i < parsedStatement.slice().rawLines().size(); i++) {
            String line = parsedStatement.slice().rawLines().get(i);
            int lineNo = parsedStatement.slice().startLine() + i;
            RoutineLineageSupport.extractLine(line, lineNo, parsedStatement, context, collector, 1_000 + (i * 100));
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
                ExpressionTokenSupport.addExpressionRows(RelationshipType.RETURN_VALUE, expression, parsedStatement, context, collector, naturalOrder++);
            } catch (JSQLParserException ignored) {
                // narrow fallback: skip unparseable return expression tokenization
            }
        }
    }

    private String extractName(List<String> declarationParts) {
        if (declarationParts == null || declarationParts.isEmpty()) {
            return ObjectRelationshipSupport.UNKNOWN_UNRESOLVED_OBJECT;
        }
        return declarationParts.get(0);
    }
}
