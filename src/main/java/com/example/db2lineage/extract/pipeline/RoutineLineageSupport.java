package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.model.ConfidenceLevel;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.RowDraft;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import com.example.db2lineage.parse.SqlStatementParser;
import com.example.db2lineage.parse.StatementSlice;
import com.example.db2lineage.resolve.SchemaMetadataService;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.Values;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.TimeKeyExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class RoutineLineageSupport {
    private static final Pattern CALL_PATTERN = Pattern.compile("(?i)^\\s*CALL\\s+([A-Z0-9_.$\\\"]+)\\s*(?:\\((.*)\\))?\\s*;?\\s*$");
    private static final Pattern FUNCTION_CALL_ASSIGNMENT = Pattern.compile("(?i)^\\s*(?:SET\\s+)?([A-Z0-9_.$]+)\\s*=\\s*([A-Z0-9_.$\\\"]+)\\s*\\((.*)\\)\\s*;?\\s*$");
    private static final Pattern DECLARE_CURSOR_PATTERN = Pattern.compile("(?i)^\\s*DECLARE\\s+([A-Z0-9_.$]+)\\s+CURSOR\\b.*$");
    private static final Pattern OPEN_CURSOR_PATTERN = Pattern.compile("(?i)^\\s*OPEN\\s+([A-Z0-9_.$]+)\\s*;?\\s*$");
    private static final Pattern CLOSE_CURSOR_PATTERN = Pattern.compile("(?i)^\\s*CLOSE\\s+([A-Z0-9_.$]+)\\s*;?\\s*$");
    private static final Pattern FETCH_CURSOR_PATTERN = Pattern.compile("(?i)^\\s*FETCH\\s+(?:NEXT\\s+FROM\\s+|FROM\\s+)?([A-Z0-9_.$]+)\\s+INTO\\s+(.+?)\\s*;?\\s*$");
    private static final Pattern GET_DIAGNOSTICS_PATTERN = Pattern.compile("(?i)^\\s*GET\\s+DIAGNOSTICS\\s+(?:(?:EXCEPTION|CONDITION)\\s+\\d+\\s+)?([A-Z0-9_.$]+)\\s*=\\s*([A-Z0-9_.$]+)\\s*;?\\s*$");
    private static final Pattern DIAGNOSTIC_TOKEN_ASSIGNMENT = Pattern.compile("(?i)^\\s*SET\\s+([A-Z0-9_.$]+)\\s*=\\s*(SQLSTATE|SQLCODE)\\s*;?\\s*$");
    private static final Pattern SPECIAL_REGISTER_ASSIGNMENT = Pattern.compile("(?i)^\\s*(?:SET\\s+)?([A-Z0-9_.$]+)\\s*=\\s*(CURRENT\\s+DATE|CURRENT\\s+TIMESTAMP|CURRENT\\s+USER|USER)\\s*;?\\s*$");
    private static final Pattern HANDLER_PATTERN = Pattern.compile("(?i)^\\s*DECLARE\\s+(?:CONTINUE|EXIT)\\s+HANDLER\\s+FOR\\s+(.+?)\\s*$");
    private static final Pattern DECLARE_GLOBAL_TEMPORARY_TABLE = Pattern.compile("(?i)^\\s*DECLARE\\s+GLOBAL\\s+TEMPORARY\\s+TABLE\\s+([A-Z0-9_.$\\\"]+).*");
    private static final Pattern DECLARE_VARIABLE_PATTERN = Pattern.compile("(?i)^\\s*DECLARE\\s+[A-Z0-9_.$]+\\s+.+$");
    private static final Pattern DECLARE_VARIABLE_WITH_OPTIONAL_DEFAULT =
            Pattern.compile("(?is)^\\s*DECLARE\\s+([A-Z0-9_.$]+)\\s+.+?(?:\\s+DEFAULT\\s+(.+?))?\\s*;?\\s*$");
    private static final Pattern EXECUTE_IMMEDIATE_PATTERN = Pattern.compile("(?i)^\\s*EXECUTE\\s+IMMEDIATE\\s+(.+?)\\s*;?\\s*$");
    private static final Pattern IF_CONDITION_PATTERN = Pattern.compile("(?i)^\\s*IF\\s+(.+?)\\s+THEN\\b.*$");
    private static final Pattern WHEN_CONDITION_PATTERN = Pattern.compile("(?i)^\\s*WHEN\\s+(.+?)\\s+THEN\\b.*$");
    private static final Pattern WHILE_CONDITION_PATTERN = Pattern.compile("(?i)^\\s*WHILE\\s+(.+?)\\s+DO\\b.*$");
    private static final Pattern UNTIL_CONDITION_PATTERN = Pattern.compile("(?i)^\\s*UNTIL\\s+(.+?)\\s*$");
    private static final Pattern TRANSACTION_CONTROL_PATTERN = Pattern.compile("(?i)^\\s*(COMMIT|ROLLBACK)\\b.*");
    private static final Pattern PROCEDURAL_HANDLER_STATEMENT_PATTERN =
            Pattern.compile("(?is)^\\s*DECLARE\\s+(?:CONTINUE|EXIT)\\s+HANDLER\\b.*$");
    private static final Pattern DECLARE_GLOBAL_TEMPORARY_TABLE_STATEMENT_PATTERN =
            Pattern.compile("(?is)^\\s*DECLARE\\s+GLOBAL\\s+TEMPORARY\\s+TABLE\\b.*$");

    private RoutineLineageSupport() {
    }

    static boolean extractLine(String line,
                               int lineNo,
                               ParsedStatementResult parsedStatement,
                               ExtractionContext context,
                               RowCollector collector,
                               int baseNaturalOrder) {
        if (line != null && line.trim().toUpperCase(Locale.ROOT).startsWith("RESULT SETS")) {
            return false;
        }
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
        if (extractDiagnosticTokenAssignment(line, lineNo, parsedStatement, context, collector, baseNaturalOrder)) {
            return true;
        }
        if (extractSpecialRegister(line, lineNo, parsedStatement, context, collector, baseNaturalOrder)) {
            return true;
        }
        if (extractHandler(line, lineNo, parsedStatement, context, collector, baseNaturalOrder)) {
            return true;
        }
        if (extractDeclareGlobalTemporaryTable(line, lineNo, parsedStatement, context, collector, baseNaturalOrder)) {
            return true;
        }
        if (extractDeclareVariable(line, lineNo, parsedStatement, context, collector, baseNaturalOrder)) {
            return true;
        }
        if (line != null && line.trim().toUpperCase(Locale.ROOT).startsWith("DECLARE")) {
            return false;
        }
        if (extractControlFlowCondition(line, lineNo, parsedStatement, context, collector, baseNaturalOrder)) {
            return true;
        }
        return extractDynamicSql(line, lineNo, parsedStatement, context, collector, baseNaturalOrder);
    }

    static boolean isTransactionControlStatement(String text) {
        return text != null && TRANSACTION_CONTROL_PATTERN.matcher(text.trim()).matches();
    }

    static void extractProceduralLinesFromSlice(ParsedStatementResult parsedStatement,
                                                ExtractionContext context,
                                                RowCollector collector) {
        int bodyStartIdx = 0;
        int bodyEndIdx = parsedStatement.slice().rawLines().size() - 1;
        for (int i = 0; i < parsedStatement.slice().rawLines().size(); i++) {
            if (parsedStatement.slice().rawLines().get(i).trim().equalsIgnoreCase("BEGIN")) {
                bodyStartIdx = i + 1;
                break;
            }
        }
        for (int i = parsedStatement.slice().rawLines().size() - 1; i >= bodyStartIdx; i--) {
            String trimmed = parsedStatement.slice().rawLines().get(i).trim();
            if (trimmed.equalsIgnoreCase("END") || trimmed.equalsIgnoreCase("END;")) {
                bodyEndIdx = i - 1;
                break;
            }
        }
        int baseOrder = 500_000;
        int absoluteLineNo = parsedStatement.slice().startLine();
        for (int i = 0; i < bodyStartIdx && i < parsedStatement.slice().rawLines().size(); i++) {
            absoluteLineNo += parsedStatement.slice().rawLines().get(i).split("\\R", -1).length;
        }
        for (int i = bodyStartIdx; i <= bodyEndIdx; i++) {
            String rawLine = parsedStatement.slice().rawLines().get(i);
            String[] physicalLines = rawLine.split("\\R", -1);
            int lineOrderOffset = 0;
            for (int physicalOffset = 0; physicalOffset < physicalLines.length; physicalOffset++) {
                String line = physicalLines[physicalOffset];
                int lineNo = absoluteLineNo + physicalOffset;
                extractLine(line, lineNo, parsedStatement, context, collector, baseOrder + i * 100 + lineOrderOffset);
                lineOrderOffset += 10;
            }
            absoluteLineNo += physicalLines.length;
        }
        extractStatementLevelProceduralMappings(parsedStatement, context, collector, baseOrder + 10_000_000, bodyStartIdx, bodyEndIdx);
    }

    private static void extractStatementLevelProceduralMappings(ParsedStatementResult parsedStatement,
                                                                ExtractionContext context,
                                                                RowCollector collector,
                                                                int baseOrder,
                                                                int bodyStartIdx,
                                                                int bodyEndIdx) {
        StringBuilder statement = new StringBuilder();
        List<String> statementLines = new ArrayList<>();
        int statementStart = -1;
        List<String> raw = parsedStatement.slice().rawLines();
        for (int i = bodyStartIdx; i <= bodyEndIdx && i < raw.size(); i++) {
            String line = raw.get(i);
            if (statementStart < 0 && !line.trim().isEmpty() && !line.trim().startsWith("--")) {
                statementStart = parsedStatement.slice().startLine() + i;
            }
            statement.append(line).append('\n');
            statementLines.add(line);
            if (!line.trim().endsWith(";")) {
                continue;
            }
            int statementEnd = parsedStatement.slice().startLine() + i;
            String text = statement.toString().trim();
            if (!text.isEmpty()) {
                if (shouldSkipStatementLevelProceduralExtraction(text)) {
                    statement.setLength(0);
                    statementLines = new ArrayList<>();
                    statementStart = -1;
                    continue;
                }
                extractSetAssignment(
                        text,
                        ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                        parsedStatement,
                        context,
                        collector,
                        baseOrder + i + 500,
                        statementStart,
                        statementEnd
                );
                extractCallStatement(text, statementStart, statementEnd, parsedStatement, context, collector, baseOrder + i + 1000);
                extractFetchStatement(text, statementStart, statementEnd, parsedStatement, context, collector, baseOrder + i + 2000);
                extractTableLevelRowsFromProceduralStatement(
                        text,
                        statementStart,
                        statementEnd,
                        parsedStatement,
                        context,
                        collector,
                        baseOrder + i + 3000
                );
                extractPredicateRelationshipsFromStatement(
                        text,
                        statementLines,
                        statementStart,
                        statementEnd,
                        parsedStatement,
                        context,
                        collector
                );
            }
            statement.setLength(0);
            statementLines = new ArrayList<>();
            statementStart = -1;
        }
    }


    private static boolean shouldSkipStatementLevelProceduralExtraction(String statementText) {
        String normalized = statementText == null ? "" : statementText.replaceAll("(?m)^\\s*--.*$", "").trim();
        if (normalized.isEmpty()) {
            return true;
        }
        if (DECLARE_GLOBAL_TEMPORARY_TABLE_STATEMENT_PATTERN.matcher(normalized).matches()) {
            return true;
        }
        if (!PROCEDURAL_HANDLER_STATEMENT_PATTERN.matcher(normalized).matches()) {
            return false;
        }
        return normalized.toUpperCase(Locale.ROOT).contains(" BEGIN");
    }
    private static void extractTableLevelRowsFromProceduralStatement(String statementText,
                                                                     int startLine,
                                                                     int endLine,
                                                                     ParsedStatementResult parsedStatement,
                                                                     ExtractionContext context,
                                                                     RowCollector collector,
                                                                     int baseOrder) {
        if (statementText == null || statementText.isBlank()) {
            return;
        }
        String normalized = statementText.replaceAll("(?m)^\\s*--.*$", "").trim();
        if (normalized.isEmpty()) {
            return;
        }
        Matcher declareCursorFor = Pattern.compile("(?is)^\\s*DECLARE\\s+[A-Z0-9_.$]+\\s+CURSOR\\s+FOR\\s+(.+?)\\s*;?\\s*$").matcher(normalized);
        if (declareCursorFor.find()) {
            normalized = declareCursorFor.group(1).trim();
        }
        Statement parsed;
        Matcher selectIntoMatcher = Pattern.compile("(?is)^\\s*SELECT\\s+(.+?)\\s+INTO\\s+(.+?)\\s+FROM\\s+(.+?)\\s*;?\\s*$")
                .matcher(normalized);
        List<String> selectIntoSources = List.of();
        List<String> selectIntoTargets = List.of();
        String selectIntoFromClause = "";
        if (selectIntoMatcher.find()) {
            selectIntoSources = splitArgs(selectIntoMatcher.group(1));
            selectIntoTargets = splitArgs(selectIntoMatcher.group(2));
            selectIntoFromClause = selectIntoMatcher.group(3).trim();
        }
        try {
            parsed = CCJSqlParserUtil.parse(normalized);
        } catch (Exception ignored) {
            if (selectIntoSources.isEmpty() || selectIntoFromClause.isBlank()) {
                return;
            }
            try {
                parsed = CCJSqlParserUtil.parse("SELECT " + String.join(", ", selectIntoSources) + " FROM " + selectIntoFromClause);
            } catch (Exception nestedIgnored) {
                return;
            }
        }

        int naturalOrder = 0;
        if (parsed instanceof Select select) {
            for (ObjectRelationshipSupport.TableRef ref : ObjectRelationshipSupport.collectSelectReadObjects(select)) {
                int lineNo = findLineInRange(parsedStatement.slice(), ref.objectName(), startLine, endLine);
                String line = parsedStatement.slice().sourceFile().getRawLine(lineNo);
                TargetObjectType targetType = resolveObjectTypeWithSessionFallback(context, ref.objectName(), ref.targetType());
                addTableLevelDraftIfAbsent(collector, lineDraft(
                        parsedStatement,
                        context,
                        "",
                        targetType,
                        ref.objectName(),
                        "",
                        RelationshipType.SELECT_TABLE,
                        lineNo,
                        line,
                        baseOrder + naturalOrder++
                ));
            }
            if (!selectIntoSources.isEmpty() && !selectIntoTargets.isEmpty()) {
                int pairs = Math.min(selectIntoSources.size(), selectIntoTargets.size());
                String owningRoutine = ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice());
                for (int i = 0; i < pairs; i++) {
                    String sourceExpr = selectIntoSources.get(i).trim();
                    String targetVar = normalizeSourceToken(selectIntoTargets.get(i).trim());
                    if (targetVar.isBlank()) {
                        continue;
                    }
                    String sourceToken = normalizeSourceToken(sourceExpr);
                    try {
                        Expression expr = CCJSqlParserUtil.parseExpression(sourceExpr);
                        if (expr instanceof Column column && column.getColumnName() != null && !column.getColumnName().isBlank()) {
                            sourceToken = column.getColumnName();
                        }
                    } catch (Exception ignored) {
                        // Keep normalized source token fallback.
                    }
                    int mapLine = findLineInRange(parsedStatement.slice(), sourceExpr, startLine, endLine);
                    String mapContent = parsedStatement.slice().sourceFile().getRawLine(mapLine);
                    collector.addDraft(parserLineDraft(
                            parsedStatement,
                            context,
                            sourceToken,
                            TargetObjectType.VARIABLE,
                            owningRoutine,
                            targetVar,
                            RelationshipType.VARIABLE_SET_MAP,
                            mapLine,
                            mapContent,
                            baseOrder + naturalOrder++
                    ));
                }
            }
            return;
        }
        if (!(parsed instanceof Insert insert)) {
            return;
        }
        String targetName = insert.getTable() == null ? ObjectRelationshipSupport.UNKNOWN_UNRESOLVED_OBJECT : insert.getTable().getFullyQualifiedName();
        TargetObjectType targetType = resolveObjectTypeWithSessionFallback(context, targetName, TargetObjectType.TABLE);
        int insertLine = findLineInRange(parsedStatement.slice(), "INSERT INTO", startLine, endLine);
        String insertContent = parsedStatement.slice().sourceFile().getRawLine(insertLine);
        addTableLevelDraftIfAbsent(collector, lineDraft(
                parsedStatement,
                context,
                "",
                targetType,
                targetName,
                "",
                RelationshipType.INSERT_TABLE,
                insertLine,
                insertContent,
                baseOrder + naturalOrder++
        ));
        if (insert.getSelect() == null) {
            return;
        }
        for (ObjectRelationshipSupport.TableRef ref : ObjectRelationshipSupport.collectSelectReadObjects(insert.getSelect())) {
            int lineNo = findLineInRange(parsedStatement.slice(), ref.objectName(), startLine, endLine);
            String line = parsedStatement.slice().sourceFile().getRawLine(lineNo);
            TargetObjectType readType = resolveObjectTypeWithSessionFallback(context, ref.objectName(), ref.targetType());
            addTableLevelDraftIfAbsent(collector, lineDraft(
                    parsedStatement,
                    context,
                    "",
                    readType,
                    ref.objectName(),
                    "",
                    RelationshipType.SELECT_TABLE,
                    lineNo,
                    line,
                    baseOrder + naturalOrder++
            ));
        }
    }

    private static void addTableLevelDraftIfAbsent(RowCollector collector, RowDraft candidate) {
        boolean exists = collector.drafts().stream().anyMatch(existing ->
                existing.sourceObjectType() == candidate.sourceObjectType()
                        && existing.sourceObject().equals(candidate.sourceObject())
                        && existing.relationship() == candidate.relationship()
                        && existing.targetObjectType() == candidate.targetObjectType()
                        && existing.targetObject().equals(candidate.targetObject())
                        && existing.lineNo() == candidate.lineNo()
                        && existing.lineContent().equals(candidate.lineContent())
        );
        if (!exists) {
            collector.addDraft(candidate);
        }
    }

    private static TargetObjectType resolveObjectTypeWithSessionFallback(ExtractionContext context,
                                                                         String objectName,
                                                                         TargetObjectType defaultType) {
        TargetObjectType resolved = context.schemaMetadataService().resolveObjectType(objectName).orElse(defaultType);
        if (resolved == TargetObjectType.TABLE
                && objectName != null
                && objectName.toUpperCase(Locale.ROOT).startsWith("SESSION.")) {
            return TargetObjectType.SESSION_TABLE;
        }
        return resolved;
    }

    private static void extractPredicateRelationshipsFromStatement(String statementText,
                                                                   List<String> statementLines,
                                                                   int startLine,
                                                                   int endLine,
                                                                   ParsedStatementResult parsedStatement,
                                                                   ExtractionContext context,
                                                                   RowCollector collector) {
        if (statementText == null || statementText.isBlank() || statementLines.isEmpty()) {
            return;
        }
        String normalized = statementText.replaceAll("(?m)^\\s*--.*$", "").trim();
        if (normalized.isEmpty()) {
            return;
        }
        String upper = normalized.toUpperCase(Locale.ROOT);
        if (!(upper.startsWith("IF ")
                || upper.startsWith("DELETE ")
                || upper.startsWith("UPDATE ")
                || upper.startsWith("MERGE ")
                || upper.startsWith("INSERT ")
                || upper.startsWith("SELECT ")
                || upper.startsWith("DECLARE "))) {
            return;
        }
        ParsedStatementResult nestedParsed = parsePredicateCandidate(
                normalized,
                statementLines,
                startLine,
                endLine,
                parsedStatement
        );
        if (nestedParsed == null || nestedParsed.statement().isEmpty()) {
            return;
        }
        Statement statement = nestedParsed.statement().orElse(null);
        if (statement == null) {
            return;
        }
        if (statement instanceof Select select) {
            addFocusedRowsFromSelect(select, nestedParsed, context, collector);
            addSelectIntoVariableMappings(
                    statementText,
                    startLine,
                    endLine,
                    parsedStatement,
                    context,
                    collector
            );
            return;
        }
        if (statement instanceof Insert insert) {
            addFocusedRowsFromInsert(insert, nestedParsed, context, collector);
            return;
        }
        if (statement instanceof Delete delete) {
            addFocusedExpressionRows(RelationshipType.WHERE, delete.getWhere(), nestedParsed, context, collector, "WHERE");
            return;
        }
        if (statement instanceof Update update) {
            addFocusedExpressionRows(RelationshipType.WHERE, update.getWhere(), nestedParsed, context, collector, "WHERE");
            OwnershipResolution updateResolution = buildOwnershipResolution(update, context, ObjectRelationshipSupport.sourceObjectName(nestedParsed.slice()));
            if (update.getExpressions() != null) {
                for (Expression expression : update.getExpressions()) {
                    if (expression instanceof ParenthesedSelect parenthesedSelect) {
                        addFocusedWhereRowsFromSelect(parenthesedSelect.getSelect(), nestedParsed, context, collector, updateResolution);
                    } else if (expression instanceof Select select) {
                        addFocusedWhereRowsFromSelect(select, nestedParsed, context, collector, updateResolution);
                    }
                }
            }
            if (update.getJoins() != null) {
                for (var join : update.getJoins()) {
                    if (join.getOnExpressions() != null) {
                        for (Expression expression : join.getOnExpressions()) {
                            addFocusedExpressionRows(RelationshipType.JOIN_ON, expression, nestedParsed, context, collector, " ON ");
                        }
                    } else {
                        addFocusedExpressionRows(RelationshipType.JOIN_ON, join.getOnExpression(), nestedParsed, context, collector, " ON ");
                    }
                }
            }
            return;
        }
        if (statement instanceof Merge merge) {
            addFocusedExpressionRows(RelationshipType.MERGE_MATCH, merge.getOnCondition(), nestedParsed, context, collector, " ON ");
            addFocusedRowsFromMerge(merge, nestedParsed, context, collector);
            OwnershipResolution mergeResolution = buildOwnershipResolution(merge, context, ObjectRelationshipSupport.sourceObjectName(nestedParsed.slice()));
            addFocusedWhereRowsFromMergeSource(merge, nestedParsed, context, collector, mergeResolution);
        }
    }

    private static void addFocusedRowsFromMerge(Merge merge,
                                                ParsedStatementResult parsedStatement,
                                                ExtractionContext context,
                                                RowCollector collector) {
        if (merge == null || merge.getTable() == null) {
            return;
        }
        String targetName = merge.getTable().getFullyQualifiedName();
        TargetObjectType targetType = resolveObjectTypeWithSessionFallback(context, targetName, TargetObjectType.TABLE);
        int naturalOrder = 0;
        int searchLine = parsedStatement.slice().startLine();
        int updateAnchor = findLineInRange(parsedStatement.slice(), "UPDATE SET", parsedStatement.slice().startLine(), parsedStatement.slice().endLine());
        if (updateAnchor > 0) {
            searchLine = Math.max(searchLine, updateAnchor);
        }
        if (merge.getMergeUpdate() != null && merge.getMergeUpdate().getUpdateSets() != null) {
            for (var updateSet : merge.getMergeUpdate().getUpdateSets()) {
                if (updateSet.getColumns() == null) {
                    continue;
                }
                for (Column column : updateSet.getColumns()) {
                    int lineNo = findMergeUpdateTargetLine(parsedStatement.slice(), column.getColumnName(), searchLine);
                    if (lineNo <= 0) {
                        MappingRelationshipSupport.addTargetColumnRow(
                                RelationshipType.MERGE_TARGET_COL,
                                targetName,
                                targetType,
                                column.getColumnName(),
                                parsedStatement,
                                context,
                                collector,
                                naturalOrder++
                        );
                        continue;
                    }
                    searchLine = Math.min(parsedStatement.slice().endLine(), lineNo + 1);
                    String rawLine = parsedStatement.slice().sourceFile().getRawLine(lineNo);
                    collector.addDraft(new RowDraft(
                            ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                            ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                            "",
                            targetType,
                            ObjectRelationshipSupport.normalizeObjectName(targetName),
                            column.getColumnName(),
                            RelationshipType.MERGE_TARGET_COL,
                            lineNo,
                            rawLine,
                            ConfidenceLevel.PARSER,
                            ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                            naturalOrder++
                    ));
                }
            }
        }
        int insertAnchor = findLineInRange(parsedStatement.slice(), "INSERT", parsedStatement.slice().startLine(), parsedStatement.slice().endLine());
        if (insertAnchor > 0) {
            searchLine = Math.max(searchLine, insertAnchor);
        }
        if (merge.getMergeInsert() != null && merge.getMergeInsert().getColumns() != null) {
            for (Column column : merge.getMergeInsert().getColumns()) {
                int lineNo = findMergeInsertTargetLine(parsedStatement.slice(), column.getColumnName(), searchLine);
                if (lineNo <= 0) {
                    MappingRelationshipSupport.addTargetColumnRow(
                            RelationshipType.MERGE_TARGET_COL,
                            targetName,
                            targetType,
                            column.getColumnName(),
                            parsedStatement,
                            context,
                            collector,
                            naturalOrder++
                    );
                    continue;
                }
                searchLine = Math.min(parsedStatement.slice().endLine(), lineNo + 1);
                String rawLine = parsedStatement.slice().sourceFile().getRawLine(lineNo);
                collector.addDraft(new RowDraft(
                        ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                        ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                        "",
                        targetType,
                        ObjectRelationshipSupport.normalizeObjectName(targetName),
                        column.getColumnName(),
                        RelationshipType.MERGE_TARGET_COL,
                        lineNo,
                        rawLine,
                        ConfidenceLevel.PARSER,
                        ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                        naturalOrder++
                ));
            }
        }
    }

    private static int findMergeUpdateTargetLine(StatementSlice slice, String columnName, int startLine) {
        if (columnName == null || columnName.isBlank()) {
            return -1;
        }
        String normalized = columnName.toUpperCase(Locale.ROOT);
        Pattern targetPattern = Pattern.compile("(?i)(?:\\b[A-Z0-9_]+\\.)?\\b" + Pattern.quote(normalized) + "\\b\\s*=");
        for (int lineNo = Math.max(slice.startLine(), startLine); lineNo <= slice.endLine(); lineNo++) {
            String line = slice.sourceFile().getRawLine(lineNo);
            String upper = line.toUpperCase(Locale.ROOT);
            if (upper.contains("WHEN NOT MATCHED")) {
                break;
            }
            if (targetPattern.matcher(upper).find()) {
                return lineNo;
            }
        }
        return -1;
    }

    private static int findMergeInsertTargetLine(StatementSlice slice, String columnName, int startLine) {
        if (columnName == null || columnName.isBlank()) {
            return -1;
        }
        String normalized = columnName.toUpperCase(Locale.ROOT);
        Pattern columnPattern = Pattern.compile("(?i)\\b" + Pattern.quote(normalized) + "\\b");
        for (int lineNo = Math.max(slice.startLine(), startLine); lineNo <= slice.endLine(); lineNo++) {
            String line = slice.sourceFile().getRawLine(lineNo);
            String upper = line.toUpperCase(Locale.ROOT);
            if (upper.contains("VALUES")) {
                break;
            }
            if (columnPattern.matcher(upper).find()) {
                return lineNo;
            }
        }
        return -1;
    }

    private static void addFocusedWhereRowsFromMergeSource(Merge merge,
                                                           ParsedStatementResult parsedStatement,
                                                           ExtractionContext context,
                                                           RowCollector collector,
                                                           OwnershipResolution outerResolution) {
        if (merge == null || merge.getFromItem() == null) {
            return;
        }
        if (merge.getFromItem() instanceof ParenthesedSelect parenthesedSelect) {
            addFocusedWhereRowsFromSelect(parenthesedSelect.getSelect(), parsedStatement, context, collector, outerResolution);
        } else if (merge.getFromItem() instanceof Select select) {
            addFocusedWhereRowsFromSelect(select, parsedStatement, context, collector, outerResolution);
        }
    }

    private static void addFocusedWhereRowsFromSelect(Select select,
                                                      ParsedStatementResult parsedStatement,
                                                      ExtractionContext context,
                                                      RowCollector collector,
                                                      OwnershipResolution outerResolution) {
        if (select == null || select.getSelectBody() == null) {
            return;
        }
        if (select.getSelectBody() instanceof PlainSelect plainSelect) {
            emitFocusedWhereFromPlainSelect(plainSelect, select, parsedStatement, context, collector, outerResolution);
            return;
        }
        if (select.getSelectBody() instanceof SetOperationList setOperationList && setOperationList.getSelects() != null) {
            for (Select branch : setOperationList.getSelects()) {
                if (branch != null && branch.getSelectBody() instanceof PlainSelect plainSelectBranch) {
                    emitFocusedWhereFromPlainSelect(plainSelectBranch, branch, parsedStatement, context, collector, outerResolution);
                }
            }
        }
    }

    private static void emitFocusedWhereFromPlainSelect(PlainSelect plainSelect,
                                                        Select ownerSelect,
                                                        ParsedStatementResult parsedStatement,
                                                        ExtractionContext context,
                                                        RowCollector collector,
                                                        OwnershipResolution outerResolution) {
        if (plainSelect == null || plainSelect.getWhere() == null) {
            return;
        }
        OwnershipResolution selectResolution = buildOwnershipResolution(ownerSelect, context, ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()));
        java.util.Map<String, TableOwner> mergedAliasOwners = new java.util.LinkedHashMap<>();
        if (outerResolution != null) {
            mergedAliasOwners.putAll(outerResolution.aliasOwners());
        }
        mergedAliasOwners.putAll(selectResolution.aliasOwners());
        TableOwner mergedSingle = selectResolution.singleTableOwner() != null
                ? selectResolution.singleTableOwner()
                : (outerResolution == null ? null : outerResolution.singleTableOwner());
        OwnershipResolution mergedResolution = new OwnershipResolution(
                mergedAliasOwners,
                mergedSingle,
                ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice())
        );
        int anchorLine = deriveAnchorLineForExpression(plainSelect.getWhere(), parsedStatement, findAnchorLine(parsedStatement.slice(), "WHERE"));
        emitFocusedPredicateRows(RelationshipType.WHERE, plainSelect.getWhere(), parsedStatement, context, collector, mergedResolution, anchorLine, 0);
    }

    private static void addFocusedRowsFromSelect(Select select,
                                                 ParsedStatementResult parsedStatement,
                                                 ExtractionContext context,
                                                 RowCollector collector) {
        if (select instanceof SetOperationList setOperationList && setOperationList.getSelects() != null) {
            for (net.sf.jsqlparser.statement.select.Select branch : setOperationList.getSelects()) {
                if (branch instanceof PlainSelect plainSelectBranch) {
                    addFocusedRowsFromPlainSelect(plainSelectBranch, parsedStatement, context, collector);
                }
            }
            return;
        }
        if (!(select instanceof PlainSelect plainSelect)) {
            return;
        }
        addFocusedRowsFromPlainSelect(plainSelect, parsedStatement, context, collector);
    }

    private static void addFocusedRowsFromInsert(Insert insert,
                                                 ParsedStatementResult parsedStatement,
                                                 ExtractionContext context,
                                                 RowCollector collector) {
        String targetName = insert.getTable() == null ? ObjectRelationshipSupport.UNKNOWN_UNRESOLVED_OBJECT : insert.getTable().getFullyQualifiedName();
        TargetObjectType targetType = context.schemaMetadataService()
                .resolveObjectType(targetName)
                .orElse(TargetObjectType.TABLE);
        if (targetType == TargetObjectType.TABLE
                && targetName != null
                && targetName.toUpperCase(Locale.ROOT).startsWith("SESSION.")) {
            targetType = TargetObjectType.SESSION_TABLE;
        }
        List<String> targetColumns = new ArrayList<>();
        if (insert.getColumns() != null && !insert.getColumns().isEmpty()) {
            insert.getColumns().forEach(c -> targetColumns.add(c.getColumnName()));
        } else {
            targetColumns.addAll(context.schemaMetadataService().resolveTargetColumnListWhenSafelyKnown(targetName));
        }
        for (int i = 0; i < targetColumns.size(); i++) {
            MappingRelationshipSupport.addTargetColumnRow(
                    RelationshipType.INSERT_TARGET_COL,
                    targetName,
                    targetType,
                    targetColumns.get(i),
                    parsedStatement,
                    context,
                    collector,
                    i
            );
        }

        Select select = insert.getSelect();
        if (select instanceof PlainSelect plainSelect && plainSelect.getSelectItems() != null && !targetColumns.isEmpty()) {
            int mappingSlots = Math.min(targetColumns.size(), plainSelect.getSelectItems().size());
            final int mappingBaseOrder = 1;
            int searchLine = parsedStatement.slice().startLine();
            for (int i = 0; i < mappingSlots; i++) {
                var selectItem = plainSelect.getSelectItems().get(i);
                if (selectItem.getExpression() == null) {
                    continue;
                }
                Expression expression = selectItem.getExpression();
                int expressionLine = findExpressionLine(parsedStatement.slice(), expression.toString(), searchLine);
                int mappingStartLine = expressionLine > 0 ? expressionLine : searchLine;
                int mappingEndLine = expressionLine > 0 ? expressionLine : parsedStatement.slice().endLine();
                if (expressionLine > 0) {
                    searchLine = expressionLine + 1;
                }
                MappingRelationshipSupport.addConciseMappingRows(
                        RelationshipType.INSERT_SELECT_MAP,
                        targetName,
                        targetType,
                        targetColumns.get(i),
                        expression,
                        parsedStatement,
                        context,
                        collector,
                        mappingBaseOrder,
                        mappingStartLine,
                        mappingEndLine
                );
            }
        } else if (!targetColumns.isEmpty()) {
            List<Expression> valuesExpressions = resolveInsertValuesExpressions(select);
            int mappingSlots = Math.min(targetColumns.size(), valuesExpressions.size());
            List<Integer> valueLines = findInsertValueLines(parsedStatement.slice());
            final int usageBaseOrder = 0;
            final int mappingBaseOrder = 1;
            for (int i = 0; i < mappingSlots; i++) {
                Expression expression = valuesExpressions.get(i);
                int valueLine = i < valueLines.size() ? valueLines.get(i) : -1;
                addCompanionSelectExprRowsForInsertValues(
                        expression,
                        parsedStatement,
                        context,
                        collector,
                        usageBaseOrder,
                        valueLine
                );
                MappingRelationshipSupport.addConciseMappingRows(
                        RelationshipType.INSERT_SELECT_MAP,
                        targetName,
                        targetType,
                        targetColumns.get(i),
                        expression,
                        parsedStatement,
                        context,
                        collector,
                        mappingBaseOrder,
                        valueLine,
                        valueLine
                );
            }
        }
        if (select != null) {
            addFocusedRowsFromSelect(select, parsedStatement, context, collector);
        }
    }

    private static List<Expression> resolveInsertValuesExpressions(Select select) {
        Select current = select;
        while (current instanceof ParenthesedSelect parenthesedSelect) {
            current = parenthesedSelect.getSelect();
        }
        if (current instanceof Values values && values.getExpressions() != null) {
            List<Expression> expressions = new ArrayList<>();
            for (Object raw : values.getExpressions()) {
                if (raw instanceof Expression expression) {
                    expressions.add(expression);
                }
            }
            return expressions;
        }
        return List.of();
    }

    private static void addFocusedRowsFromPlainSelect(PlainSelect plainSelect,
                                                      ParsedStatementResult parsedStatement,
                                                      ExtractionContext context,
                                                      RowCollector collector) {
        if (plainSelect.getSelectItems() != null) {
            int projectionOrder = 0;
            int expressionSearchLine = parsedStatement.slice().startLine();
            for (var selectItem : plainSelect.getSelectItems()) {
                if (selectItem == null || selectItem.getExpression() == null) {
                    continue;
                }
                Expression expression = selectItem.getExpression();
                if (expression instanceof Column column) {
                    boolean variableLikeColumn = (column.getTable() == null || column.getTable().getName() == null || column.getTable().getName().isBlank())
                            && looksLikeVariable(column.getColumnName());
                    if (variableLikeColumn) {
                        OwnershipResolution resolution = buildOwnershipResolution(
                                plainSelect,
                                context,
                                ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice())
                        );
                        expressionSearchLine = addSelectExpressionRows(
                                expression,
                                parsedStatement,
                                context,
                                collector,
                                projectionOrder++,
                                resolution,
                                expressionSearchLine
                        ) + 1;
                        continue;
                    }
                    OwnershipResolution resolution = buildOwnershipResolution(
                            plainSelect,
                            context,
                            ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice())
                    );
                    String anchorToken = column.getFullyQualifiedName() == null || column.getFullyQualifiedName().isBlank()
                            ? column.getColumnName()
                            : column.getFullyQualifiedName();
                    int columnLine = findExpressionLine(parsedStatement.slice(), anchorToken, expressionSearchLine);
                    addSelectFieldRow(
                            column,
                            parsedStatement,
                            context,
                            collector,
                            projectionOrder++,
                            resolution,
                            columnLine
                    );
                    if (columnLine > 0) {
                        expressionSearchLine = columnLine + 1;
                    }
                } else {
                    OwnershipResolution resolution = buildOwnershipResolution(
                            plainSelect,
                            context,
                            ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice())
                    );
                    expressionSearchLine = addSelectExpressionRows(
                            expression,
                            parsedStatement,
                            context,
                            collector,
                            projectionOrder++,
                            resolution,
                            expressionSearchLine
                    ) + 1;
                }
            }
        }
        addFocusedExpressionRows(RelationshipType.WHERE, plainSelect.getWhere(), parsedStatement, context, collector, "WHERE");
        if (plainSelect.getJoins() == null) {
            return;
        }
        for (var join : plainSelect.getJoins()) {
            if (join.getOnExpressions() != null) {
                for (Expression expression : join.getOnExpressions()) {
                    addFocusedExpressionRows(RelationshipType.JOIN_ON, expression, parsedStatement, context, collector, " ON ");
                }
            } else {
                addFocusedExpressionRows(RelationshipType.JOIN_ON, join.getOnExpression(), parsedStatement, context, collector, " ON ");
            }
        }
    }

    private static void addFocusedExpressionRows(RelationshipType relationship,
                                                 Expression expression,
                                                 ParsedStatementResult parsedStatement,
                                                 ExtractionContext context,
                                                 RowCollector collector,
                                                 String anchorToken) {
        if (expression == null) {
            return;
        }
        StatementSlice slice = parsedStatement.slice();
        int anchorLine = deriveAnchorLineForExpression(expression, parsedStatement, findAnchorLine(slice, anchorToken));
        OwnershipResolution resolution = buildOwnershipResolution(parsedStatement.statement().orElse(null), context, ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()));
        emitFocusedPredicateRows(
                relationship,
                expression,
                parsedStatement,
                context,
                collector,
                resolution,
                anchorLine,
                0
        );
    }

    private static int findAnchorLine(StatementSlice slice, String anchorToken) {
        String needle = anchorToken == null ? "" : anchorToken.toUpperCase(Locale.ROOT).trim();
        for (int line = slice.startLine(); line <= slice.endLine(); line++) {
            String raw = slice.sourceFile().getRawLine(line).toUpperCase(Locale.ROOT);
            if (needle.isBlank()) {
                continue;
            }
            if (needle.equals("WHERE")) {
                if (raw.matches(".*\\bWHERE\\b.*")) {
                    return line;
                }
                continue;
            }
            if (needle.equals("ON")) {
                if (raw.matches(".*\\bON\\b.*")) {
                    return line;
                }
                continue;
            }
            if (raw.contains(needle)) {
                return line;
            }
        }
        return slice.startLine();
    }

    private static int emitFocusedPredicateRows(RelationshipType relationship,
                                                Expression expression,
                                                ParsedStatementResult parsedStatement,
                                                ExtractionContext context,
                                                RowCollector collector,
                                                OwnershipResolution resolution,
                                                int anchorLine,
                                                int naturalOrder) {
        if (expression == null) {
            return naturalOrder;
        }
        if (expression instanceof AndExpression andExpression) {
            int leftAnchor = deriveAnchorLineForExpression(andExpression.getLeftExpression(), parsedStatement, anchorLine);
            int rightAnchor = deriveAnchorLineForExpression(andExpression.getRightExpression(), parsedStatement, anchorLine);
            naturalOrder = emitFocusedPredicateRows(relationship, andExpression.getLeftExpression(), parsedStatement, context, collector, resolution, leftAnchor, naturalOrder);
            return emitFocusedPredicateRows(relationship, andExpression.getRightExpression(), parsedStatement, context, collector, resolution, rightAnchor, naturalOrder);
        }
        if (expression instanceof OrExpression orExpression) {
            int leftAnchor = deriveAnchorLineForExpression(orExpression.getLeftExpression(), parsedStatement, anchorLine);
            int rightAnchor = deriveAnchorLineForExpression(orExpression.getRightExpression(), parsedStatement, anchorLine);
            naturalOrder = emitFocusedPredicateRows(relationship, orExpression.getLeftExpression(), parsedStatement, context, collector, resolution, leftAnchor, naturalOrder);
            return emitFocusedPredicateRows(relationship, orExpression.getRightExpression(), parsedStatement, context, collector, resolution, rightAnchor, naturalOrder);
        }
        if (expression instanceof ParenthesedExpressionList<?> parenthesedExpressionList
                && parenthesedExpressionList.size() == 1) {
            Expression innerExpression = (Expression) parenthesedExpressionList.get(0);
            int innerAnchor = deriveAnchorLineForExpression(innerExpression, parsedStatement, anchorLine);
            return emitFocusedPredicateRows(relationship, innerExpression, parsedStatement, context, collector, resolution, innerAnchor, naturalOrder);
        }
        if (expression instanceof Parenthesis parenthesis) {
            int innerAnchor = deriveAnchorLineForExpression(parenthesis.getExpression(), parsedStatement, anchorLine);
            return emitFocusedPredicateRows(relationship, parenthesis.getExpression(), parsedStatement, context, collector, resolution, innerAnchor, naturalOrder);
        }
        if (expression instanceof ExistsExpression existsExpression) {
            ExistsContext existsContext = resolveExistsContext(existsExpression, context, resolution);
            if (existsContext != null && existsContext.whereExpression() != null) {
                int existsAnchor = deriveAnchorLineForExpression(existsContext.whereExpression(), parsedStatement, anchorLine);
                return emitFocusedPredicateRows(
                        relationship,
                        existsContext.whereExpression(),
                        parsedStatement,
                        context,
                        collector,
                        existsContext.resolution(),
                        existsAnchor,
                        naturalOrder
                );
            }
        }
        if (expression instanceof IsNullExpression isNullExpression) {
            int predicateAnchor = deriveAnchorLineForExpression(isNullExpression.getLeftExpression(), parsedStatement, anchorLine);
            PredicateTerm left = resolvePredicateTerm(isNullExpression.getLeftExpression(), resolution);
            naturalOrder = addPredicateTermRow(relationship, left, parsedStatement, context, collector, predicateAnchor, naturalOrder);
            PredicateTerm nullTerm = new PredicateTerm("CONSTANT:NULL", left.targetType(), left.targetObject(), left.targetField());
            return addPredicateTermRow(relationship, nullTerm, parsedStatement, context, collector, predicateAnchor, naturalOrder);
        }
        if (expression instanceof InExpression inExpression) {
            int predicateAnchor = deriveAnchorLineForExpression(inExpression.getLeftExpression(), parsedStatement, anchorLine);
            PredicateTerm left = resolvePredicateTerm(inExpression.getLeftExpression(), resolution);
            naturalOrder = addPredicateTermRow(relationship, left, parsedStatement, context, collector, predicateAnchor, naturalOrder);
            if (inExpression.getRightExpression() instanceof ExpressionList<?> list && list.getExpressions() != null) {
                for (Expression item : list.getExpressions()) {
                    PredicateTerm right = resolvePredicateTerm(item, resolution);
                    if (right.sourceField().startsWith("CONSTANT:") && left.isConcreteTarget()) {
                        right = new PredicateTerm(right.sourceField(), left.targetType(), left.targetObject(), left.targetField());
                    }
                    naturalOrder = addPredicateTermRow(relationship, right, parsedStatement, context, collector, predicateAnchor, naturalOrder);
                }
            }
            return naturalOrder;
        }
        if (expression instanceof BinaryExpression binaryExpression) {
            int predicateAnchor = deriveAnchorLineForExpression(binaryExpression.getLeftExpression(), parsedStatement, anchorLine);
            PredicateTerm left = resolvePredicateTerm(binaryExpression.getLeftExpression(), resolution);
            PredicateTerm right = resolvePredicateTerm(binaryExpression.getRightExpression(), resolution);
            naturalOrder = addPredicateTermRow(relationship, left, parsedStatement, context, collector, predicateAnchor, naturalOrder);
            if (right.sourceField().startsWith("CONSTANT:") && left.isConcreteTarget()) {
                right = new PredicateTerm(right.sourceField(), left.targetType(), left.targetObject(), left.targetField());
            } else if (left.sourceField().startsWith("CONSTANT:") && right.isConcreteTarget()) {
                left = new PredicateTerm(left.sourceField(), right.targetType(), right.targetObject(), right.targetField());
            }
            return addPredicateTermRow(relationship, right, parsedStatement, context, collector, predicateAnchor, naturalOrder);
        }
        PredicateTerm generic = resolvePredicateTerm(expression, resolution);
        return addPredicateTermRow(relationship, generic, parsedStatement, context, collector, anchorLine, naturalOrder);
    }

    private static ExistsContext resolveExistsContext(ExistsExpression existsExpression,
                                                      ExtractionContext context,
                                                      OwnershipResolution outerResolution) {
        if (existsExpression == null || existsExpression.getRightExpression() == null) {
            return null;
        }
        Select nestedSelect = null;
        if (existsExpression.getRightExpression() instanceof ParenthesedSelect parenthesedSelect) {
            nestedSelect = parenthesedSelect.getSelect();
        } else if (existsExpression.getRightExpression() instanceof Select select) {
            nestedSelect = select;
        }
        if (nestedSelect == null || !(nestedSelect.getSelectBody() instanceof PlainSelect plainSelect)) {
            return null;
        }
        OwnershipResolution nestedResolution = buildOwnershipResolution(nestedSelect, context, outerResolution.routineName());
        java.util.Map<String, TableOwner> mergedAliasOwners = new java.util.LinkedHashMap<>(outerResolution.aliasOwners());
        mergedAliasOwners.putAll(nestedResolution.aliasOwners());
        OwnershipResolution merged = new OwnershipResolution(
                mergedAliasOwners,
                nestedResolution.singleTableOwner() != null ? nestedResolution.singleTableOwner() : outerResolution.singleTableOwner(),
                outerResolution.routineName()
        );
        return new ExistsContext(plainSelect.getWhere(), merged);
    }

    private static int deriveAnchorLineForExpression(Expression expression,
                                                     ParsedStatementResult parsedStatement,
                                                     int fallbackLine) {
        if (expression == null) {
            return fallbackLine;
        }
        List<ExpressionTokenSupport.TokenUse> uses = ExpressionTokenSupport.collect(
                expression,
                parsedStatement.slice(),
                fallbackLine,
                parsedStatement.slice().endLine()
        );
        return uses.stream().mapToInt(ExpressionTokenSupport.TokenUse::lineNo).min().orElse(fallbackLine);
    }

    private static int addPredicateTermRow(RelationshipType relationship,
                                           PredicateTerm term,
                                           ParsedStatementResult parsedStatement,
                                           ExtractionContext context,
                                           RowCollector collector,
                                           int anchorLine,
                                           int naturalOrder) {
        if (term == null || term.sourceField().isBlank()) {
            return naturalOrder;
        }
        String lineContent = parsedStatement.slice().sourceFile().getRawLine(anchorLine);
        collector.addDraft(new RowDraft(
                ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                term.sourceField(),
                term.targetType(),
                term.targetObject(),
                term.targetField(),
                relationship,
                anchorLine,
                lineContent,
                ConfidenceLevel.PARSER,
                ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                naturalOrder
        ));
        return naturalOrder + 1;
    }

    private static PredicateTerm resolvePredicateTerm(Expression expression, OwnershipResolution resolution) {
        if (expression == null) {
            return new PredicateTerm("", TargetObjectType.UNKNOWN, ObjectRelationshipSupport.UNKNOWN_UNRESOLVED_OBJECT, "");
        }
        if (expression instanceof Column column) {
            String full = column.getFullyQualifiedName();
            String source = column.getColumnName();
            String qualifier = column.getTable() == null ? "" : column.getTable().getName();
            if (qualifier != null && !qualifier.isBlank()) {
                TableOwner owner = resolution.aliasOwners().get(qualifier.toUpperCase(Locale.ROOT));
                if (owner != null) {
                    return new PredicateTerm(source, owner.type(), owner.objectName(), source);
                }
            }
            if (resolution.singleTableOwner() != null && !looksLikeVariable(source)) {
                return new PredicateTerm(source, resolution.singleTableOwner().type(), resolution.singleTableOwner().objectName(), source);
            }
            return new PredicateTerm(source, TargetObjectType.VARIABLE, resolution.routineName(), source);
        }
        String raw = expression.toString();
        String normalized = normalizeSourceToken(raw);
        if (normalized.startsWith("CONSTANT:")) {
            return new PredicateTerm(normalized, TargetObjectType.UNKNOWN, ObjectRelationshipSupport.UNKNOWN_UNRESOLVED_OBJECT, "");
        }
        if (looksLikeVariable(raw)) {
            String variable = raw.trim();
            return new PredicateTerm(variable, TargetObjectType.VARIABLE, resolution.routineName(), variable);
        }
        return new PredicateTerm(normalized, TargetObjectType.UNKNOWN, ObjectRelationshipSupport.UNKNOWN_UNRESOLVED_OBJECT, "");
    }

    private static boolean looksLikeVariable(String token) {
        if (token == null) {
            return false;
        }
        String t = token.trim().toUpperCase(Locale.ROOT);
        return t.matches("^(P_|LV_|LD_|LN_|AT_|CV_).+");
    }

    private static OwnershipResolution buildOwnershipResolution(Statement statement, ExtractionContext context, String routineName) {
        java.util.Map<String, TableOwner> aliasOwners = new java.util.LinkedHashMap<>();
        TableOwner single = null;
        if (statement instanceof Select select && select.getSelectBody() instanceof PlainSelect plainSelect) {
            single = addFromItemOwners(plainSelect.getFromItem(), aliasOwners, context);
            if (plainSelect.getJoins() != null) {
                for (Join join : plainSelect.getJoins()) {
                    addFromItemOwners(join.getFromItem(), aliasOwners, context);
                }
            }
        } else if (statement instanceof Insert insert) {
            if (insert.getSelect() instanceof PlainSelect plainSelect) {
                single = addFromItemOwners(plainSelect.getFromItem(), aliasOwners, context);
                if (plainSelect.getJoins() != null) {
                    for (Join join : plainSelect.getJoins()) {
                        addFromItemOwners(join.getFromItem(), aliasOwners, context);
                    }
                }
            }
        } else if (statement instanceof Delete delete) {
            if (delete.getTable() != null) {
                single = addTableOwner(delete.getTable(), aliasOwners, context);
            }
        } else if (statement instanceof Update update) {
            if (update.getTable() != null) {
                single = addTableOwner(update.getTable(), aliasOwners, context);
            }
        } else if (statement instanceof Merge merge) {
            if (merge.getTable() != null) {
                single = addTableOwner(merge.getTable(), aliasOwners, context);
            }
            addFromItemOwners(merge.getFromItem(), aliasOwners, context);
        }
        return new OwnershipResolution(aliasOwners, single, routineName);
    }

    private static TableOwner addFromItemOwners(FromItem fromItem,
                                                java.util.Map<String, TableOwner> aliasOwners,
                                                ExtractionContext context) {
        if (fromItem instanceof Table table) {
            return addTableOwner(table, aliasOwners, context);
        }
        if (fromItem instanceof ParenthesedSelect parenthesedSelect && parenthesedSelect.getSelect() != null) {
            Select select = parenthesedSelect.getSelect();
            if (select.getSelectBody() instanceof PlainSelect plainSelect) {
                TableOwner single = addFromItemOwners(plainSelect.getFromItem(), aliasOwners, context);
                if (plainSelect.getJoins() != null) {
                    for (Join join : plainSelect.getJoins()) {
                        addFromItemOwners(join.getFromItem(), aliasOwners, context);
                    }
                }
                return single;
            }
        }
        return null;
    }

    private static TableOwner addTableOwner(Table table,
                                            java.util.Map<String, TableOwner> aliasOwners,
                                            ExtractionContext context) {
        String object = table.getFullyQualifiedName();
        TargetObjectType type = context.schemaMetadataService().resolveObjectType(object)
                .orElse(object != null && object.toUpperCase(Locale.ROOT).startsWith("SESSION.") ? TargetObjectType.SESSION_TABLE : TargetObjectType.TABLE);
        TableOwner owner = new TableOwner(ObjectRelationshipSupport.normalizeObjectName(object), type);
        aliasOwners.put(table.getName().toUpperCase(Locale.ROOT), owner);
        if (table.getAlias() != null && table.getAlias().getName() != null) {
            aliasOwners.put(table.getAlias().getName().toUpperCase(Locale.ROOT), owner);
        }
        return owner;
    }

    private record TableOwner(String objectName, TargetObjectType type) {
    }

    private record OwnershipResolution(java.util.Map<String, TableOwner> aliasOwners,
                                       TableOwner singleTableOwner,
                                       String routineName) {
        private TableOwner resolve(String qualifier) {
            if (qualifier == null || qualifier.isBlank()) {
                return null;
            }
            return aliasOwners.get(qualifier.toUpperCase(Locale.ROOT));
        }

        private Optional<TableOwner> singleOwner() {
            return Optional.ofNullable(singleTableOwner);
        }
    }

    private record SelectExprTarget(String sourceField,
                                    TargetObjectType targetType,
                                    String targetObject,
                                    String targetField) {
    }

    private record PredicateTerm(String sourceField,
                                 TargetObjectType targetType,
                                 String targetObject,
                                 String targetField) {
        private boolean isConcreteTarget() {
            return targetType != TargetObjectType.UNKNOWN && targetObject != null && !targetObject.isBlank() && targetField != null;
        }
    }

    private record ExistsContext(Expression whereExpression, OwnershipResolution resolution) {
    }

    private static ParsedStatementResult parsePredicateCandidate(String normalizedStatementText,
                                                                 List<String> statementLines,
                                                                 int startLine,
                                                                 int endLine,
                                                                 ParsedStatementResult parsedStatement) {
        String parseText = normalizedStatementText;
        Matcher declareCursor = Pattern.compile("(?is)^\\s*DECLARE\\s+[A-Z0-9_.$]+\\s+CURSOR\\s+FOR\\s+(.+?)\\s*;?\\s*$")
                .matcher(parseText);
        if (declareCursor.find()) {
            parseText = declareCursor.group(1).trim();
        }
        ParsedStatementResult directParsed = parsePredicateStatementSlice(
                parseText,
                statementLines,
                startLine,
                endLine,
                parsedStatement
        );
        if (directParsed.statement().isPresent()) {
            return directParsed;
        }
        Matcher selectInto = Pattern.compile("(?is)^\\s*SELECT\\s+(.+?)\\s+INTO\\s+.+?\\s+FROM\\s+(.+)$").matcher(parseText);
        if (selectInto.find()) {
            ParsedStatementResult rewritten = parsePredicateStatementSlice(
                    "SELECT " + selectInto.group(1).trim() + " FROM " + selectInto.group(2).trim(),
                    statementLines,
                    startLine,
                    endLine,
                    parsedStatement
            );
            if (rewritten.statement().isPresent()) {
                return rewritten;
            }
        }
        Matcher conditionalSql = Pattern.compile("(?is)^\\s*IF\\b.+?\\bTHEN\\s+(MERGE\\b.+)$").matcher(parseText);
        if (conditionalSql.find()) {
            return parsePredicateStatementSlice(
                    conditionalSql.group(1).trim(),
                    statementLines,
                    startLine,
                    endLine,
                    parsedStatement
            );
        }
        return directParsed;
    }

    private static ParsedStatementResult parsePredicateStatementSlice(String statementText,
                                                                      List<String> statementLines,
                                                                      int startLine,
                                                                      int endLine,
                                                                      ParsedStatementResult parsedStatement) {
        StatementSlice nestedSlice = new StatementSlice(
                parsedStatement.slice().sourceFile(),
                parsedStatement.slice().sourceCategory(),
                statementText,
                startLine,
                endLine,
                List.copyOf(statementLines),
                parsedStatement.slice().ordinalWithinFile() * 1_000_000
                        + Math.max(0, startLine - parsedStatement.slice().startLine())
        );
        return new SqlStatementParser().parse(nestedSlice);
    }

    private static boolean extractCallStatement(String statementText,
                                                int startLine,
                                                int endLine,
                                                ParsedStatementResult parsedStatement,
                                                ExtractionContext context,
                                                RowCollector collector,
        int baseOrder) {
        String normalized = statementText.replaceAll("(?m)^\\s*--.*$", "").trim();
        Matcher call = Pattern.compile("(?is)^\\s*CALL\\s+([A-Z0-9_.$\\\"]+)\\s*\\((.*)\\)\\s*;?\\s*$").matcher(normalized);
        boolean embeddedCallFallback = false;
        if (!call.find()) {
            call = Pattern.compile("(?is)\\bCALL\\s+([A-Z0-9_.$\\\"]+)\\s*\\((.*?)\\)\\s*;").matcher(normalized);
            if (!call.find()) {
                return false;
            }
            embeddedCallFallback = true;
        }
        String callable = call.group(1).trim();
        List<String> args = splitArgs(call.group(2));
        int callLineNo = findLineInRange(parsedStatement.slice(), "CALL " + callable, startLine, endLine);
        String callLine = parsedStatement.slice().sourceFile().getRawLine(callLineNo);
        collector.addDraft(parserLineDraft(parsedStatement, context, "", TargetObjectType.PROCEDURE, callable, "",
                RelationshipType.CALL_PROCEDURE, callLineNo, callLine, baseOrder));
        for (int i = 0; i < args.size(); i++) {
            String arg = args.get(i).trim();
            int lineNo = findLineInRange(parsedStatement.slice(), arg, startLine, endLine);
            String line = parsedStatement.slice().sourceFile().getRawLine(lineNo);
            String targetField = embeddedCallFallback
                    ? positionalTargetFieldForEmbeddedCall(i)
                    : positionalTargetField(RelationshipType.CALL_PARAM_MAP, i);
            collector.addDraft(parserLineDraft(parsedStatement, context, normalizeSourceToken(arg), TargetObjectType.PROCEDURE, callable,
                    targetField, RelationshipType.CALL_PARAM_MAP, lineNo, line, baseOrder + 10 + i));
        }
        return true;
    }

    private static boolean extractFetchStatement(String statementText,
                                                 int startLine,
                                                 int endLine,
                                                 ParsedStatementResult parsedStatement,
                                                 ExtractionContext context,
                                                 RowCollector collector,
                                                 int baseOrder) {
        String normalized = statementText.replaceAll("(?m)^\\s*--.*$", "").trim();
        Matcher fetch = Pattern.compile("(?is)^\\s*(?:[A-Z0-9_.$]+\\s*:\\s*)?(?:LOOP\\s+)?FETCH\\s+(?:NEXT\\s+FROM\\s+|FROM\\s+)?([A-Z0-9_.$]+)\\s+INTO\\s+(.+?)\\s*;?\\s*$").matcher(normalized);
        if (!fetch.find()) {
            return false;
        }
        String cursor = fetch.group(1).trim();
        String owningRoutine = ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice());
        int fetchLineNo = findLineInRange(parsedStatement.slice(), "FETCH " + cursor, startLine, endLine);
        String fetchLine = parsedStatement.slice().sourceFile().getRawLine(fetchLineNo);
        collector.addDraft(lineDraft(parsedStatement, context, "", TargetObjectType.CURSOR, cursor, "FETCH",
                RelationshipType.CURSOR_READ, fetchLineNo, fetchLine, baseOrder));
        List<String> targets = splitArgs(fetch.group(2));
        List<String> selectSources = resolveCursorSelectSources(cursor, parsedStatement);
        for (int i = 0; i < targets.size(); i++) {
            String sourceField = i < selectSources.size() ? selectSources.get(i) : cursor;
            String target = targets.get(i).trim();
            int lineNo = findLineInRange(parsedStatement.slice(), target, startLine, endLine);
            String line = parsedStatement.slice().sourceFile().getRawLine(lineNo);
            collector.addDraft(lineDraft(parsedStatement, context, sourceField, TargetObjectType.VARIABLE, owningRoutine, target,
                    RelationshipType.CURSOR_FETCH_MAP, lineNo, line, baseOrder + 10 + i));
        }
        return true;
    }

    private static int findLineInRange(StatementSlice slice, String token, int startLine, int endLine) {
        String needle = token == null ? "" : token.trim().toUpperCase(Locale.ROOT);
        if (needle.isEmpty()) {
            return startLine;
        }
        for (int lineNo = startLine; lineNo <= endLine; lineNo++) {
            String line = slice.sourceFile().getRawLine(lineNo).toUpperCase(Locale.ROOT);
            if (line.contains(needle)) {
                return lineNo;
            }
        }
        String unquotedNeedle = needle.replace("\"", "");
        if (!unquotedNeedle.equals(needle)) {
            for (int lineNo = startLine; lineNo <= endLine; lineNo++) {
                String line = slice.sourceFile().getRawLine(lineNo).toUpperCase(Locale.ROOT).replace("\"", "");
                if (line.contains(unquotedNeedle)) {
                    return lineNo;
                }
            }
        }
        int dot = unquotedNeedle.lastIndexOf('.');
        if (dot > 0 && dot < unquotedNeedle.length() - 1) {
            String suffixNeedle = unquotedNeedle.substring(dot + 1);
            for (int lineNo = startLine; lineNo <= endLine; lineNo++) {
                String line = slice.sourceFile().getRawLine(lineNo).toUpperCase(Locale.ROOT).replace("\"", "");
                if (line.contains(suffixNeedle)) {
                    return lineNo;
                }
            }
        }
        return startLine;
    }

    private static void addSelectIntoVariableMappings(String statementText,
                                                      int startLine,
                                                      int endLine,
                                                      ParsedStatementResult parsedStatement,
                                                      ExtractionContext context,
                                                      RowCollector collector) {
        if (statementText == null || statementText.isBlank()) {
            return;
        }
        Matcher selectInto = Pattern.compile("(?is)^\\s*SELECT\\s+(.+?)\\s+INTO\\s+(.+?)\\s+FROM\\b.*$").matcher(statementText);
        if (!selectInto.find()) {
            return;
        }
        List<String> selectItems = splitArgs(selectInto.group(1));
        List<String> intoTargets = splitArgs(selectInto.group(2));
        int slotCount = Math.min(selectItems.size(), intoTargets.size());
        if (slotCount <= 0) {
            return;
        }
        String owningRoutine = ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice());
        for (int i = 0; i < slotCount; i++) {
            String selectItem = selectItems.get(i) == null ? "" : selectItems.get(i).trim();
            String targetVariable = intoTargets.get(i) == null ? "" : intoTargets.get(i).trim();
            if (selectItem.isEmpty() || targetVariable.isEmpty()) {
                continue;
            }
            int lineNo = findLineInRange(parsedStatement.slice(), selectItem, startLine, endLine);
            String lineContent = parsedStatement.slice().sourceFile().getRawLine(lineNo);
            try {
                Expression expression = CCJSqlParserUtil.parseExpression(selectItem);
                MappingRelationshipSupport.addConciseMappingRows(
                        RelationshipType.VARIABLE_SET_MAP,
                        owningRoutine,
                        TargetObjectType.VARIABLE,
                        targetVariable,
                        expression,
                        parsedStatement,
                        context,
                        collector,
                        1,
                        lineNo,
                        lineNo
                );
            } catch (JSQLParserException ignored) {
                collector.addDraft(new RowDraft(
                        ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                        owningRoutine,
                        normalizeSourceToken(selectItem),
                        TargetObjectType.VARIABLE,
                        owningRoutine,
                        targetVariable,
                        RelationshipType.VARIABLE_SET_MAP,
                        lineNo,
                        lineContent,
                        ConfidenceLevel.PARSER,
                        ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                        1
                ));
            }
        }
    }

    static boolean isPlainVariableDeclaration(String text) {
        if (text == null) {
            return false;
        }
        String trimmed = firstSqlLine(text);
        if (!DECLARE_VARIABLE_PATTERN.matcher(trimmed).matches()) {
            return false;
        }
        return !DECLARE_CURSOR_PATTERN.matcher(trimmed).matches()
                && !DECLARE_GLOBAL_TEMPORARY_TABLE.matcher(trimmed).matches()
                && !HANDLER_PATTERN.matcher(trimmed).matches();
    }

    static boolean isRoutineStructuralStatement(String text) {
        if (text == null) {
            return false;
        }
        String first = firstSqlLine(text).toUpperCase(Locale.ROOT);
        return first.equals("END;")
                || first.equals("END")
                || first.equals("ELSE")
                || first.equals("ELSE;")
                || first.startsWith("END IF")
                || first.startsWith("END WHILE")
                || first.startsWith("END FOR");
    }

    private static String firstSqlLine(String text) {
        for (String line : text.split("\\R")) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("--")) {
                continue;
            }
            return trimmed;
        }
        return text.trim();
    }

    private static boolean extractControlFlowCondition(String line,
                                                       int lineNo,
                                                       ParsedStatementResult parsedStatement,
                                                       ExtractionContext context,
                                                       RowCollector collector,
                                                       int baseNaturalOrder) {
        String condition = firstMatchedGroup(line, IF_CONDITION_PATTERN, WHILE_CONDITION_PATTERN, UNTIL_CONDITION_PATTERN);
        if (condition == null || condition.isBlank()) {
            String upper = line == null ? "" : line.trim().toUpperCase(Locale.ROOT);
            if (upper.startsWith("WHEN MATCHED") || upper.startsWith("WHEN NOT MATCHED")) {
                return false;
            }
            condition = firstMatchedGroup(line, WHEN_CONDITION_PATTERN);
            if (condition != null && condition.contains(".")) {
                return false;
            }
        }
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

        List<ExpressionTokenSupport.TokenUse> tokens = ExpressionTokenSupport.collect(expression, parsedStatement.slice(), lineNo, lineNo);
        if (tokens.isEmpty()) {
            return false;
        }

        boolean added = false;
        for (ExpressionTokenSupport.TokenUse tokenUse : tokens) {
            collector.addDraft(lineDraft(
                    parsedStatement,
                    context,
                    tokenUse.token(),
                    targetType,
                    targetObject,
                    controlFlowTargetField(tokenUse.token()),
                    RelationshipType.CONTROL_FLOW_CONDITION,
                    lineNo,
                    line,
                    baseNaturalOrder + tokenUse.orderOnLine()
            ));
            added = true;
        }
        return added;
    }

    private static String controlFlowTargetField(String token) {
        if (token == null || token.isBlank() || token.startsWith("CONSTANT:")) {
            return "";
        }
        String trimmed = token.trim();
        int dot = trimmed.lastIndexOf('.');
        return dot >= 0 ? trimmed.substring(dot + 1) : trimmed;
    }

    @SafeVarargs
    private static String firstMatchedGroup(String line, Pattern... patterns) {
        for (Pattern pattern : patterns) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.matches()) {
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
        collector.addDraft(parserLineDraft(parsedStatement, context, "", TargetObjectType.PROCEDURE, callable, "",
                RelationshipType.CALL_PROCEDURE, lineNo, line, baseNaturalOrder));
        addParameterRows(parsedStatement, context, collector, lineNo, line, callable, args, RelationshipType.CALL_PARAM_MAP, baseNaturalOrder + 10);
        return true;
    }

    private static boolean extractFunctionAssignment(String line, int lineNo, ParsedStatementResult parsedStatement,
                                                     ExtractionContext context, RowCollector collector, int baseNaturalOrder) {
        Matcher assignment = FUNCTION_CALL_ASSIGNMENT.matcher(line);
        if (!assignment.matches()) {
            return false;
        }
        String targetVariable = assignment.group(1).trim();
        String functionName = assignment.group(2).trim();
        String owningRoutine = ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice());
        List<String> args = splitArgs(assignment.group(3));
        collector.addDraft(parserLineDraft(
                parsedStatement,
                context,
                "",
                TargetObjectType.FUNCTION,
                functionName,
                "",
                RelationshipType.CALL_FUNCTION,
                lineNo,
                line,
                baseNaturalOrder
        ));
        addParameterRows(parsedStatement, context, collector, lineNo, line, functionName, args, RelationshipType.FUNCTION_PARAM_MAP, baseNaturalOrder);
        boolean hasFunctionExprMapOnLine = collector.drafts().stream().anyMatch(existing ->
                existing.relationship() == RelationshipType.FUNCTION_EXPR_MAP
                        && existing.lineNo() == lineNo
                        && existing.targetObjectType() == TargetObjectType.VARIABLE
                        && owningRoutine.equals(existing.targetObject())
                        && targetVariable.equals(existing.targetField())
                        && functionName.equals(existing.sourceField())
        );
        if (hasFunctionExprMapOnLine) {
            return true;
        }
        collector.addDraft(lineDraft(parsedStatement, context, functionName, TargetObjectType.VARIABLE, owningRoutine, targetVariable,
                RelationshipType.FUNCTION_EXPR_MAP, lineNo, line, baseNaturalOrder + 1));
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
            String targetField = useNamed && i < names.size() ? names.get(i) : positionalTargetField(relationshipType, i);
            String sourceToken = normalizeSourceToken(arg);
            RowDraft row = relationshipType == RelationshipType.CALL_PARAM_MAP
                    ? parserLineDraft(parsedStatement, context, sourceToken, TargetObjectType.PROCEDURE,
                    callable, targetField, relationshipType, lineNo, line, baseNaturalOrder + i)
                    : lineDraft(parsedStatement, context, sourceToken, TargetObjectType.FUNCTION,
                    callable, targetField, relationshipType, lineNo, line, baseNaturalOrder + i);
            collector.addDraft(row);
        }
    }

    private static String positionalTargetField(RelationshipType relationshipType, int index) {
        String slot = "$" + (index + 1);
        if (relationshipType == RelationshipType.CALL_PARAM_MAP) {
            return slot + " ";
        }
        return slot;
    }

    private static String positionalTargetFieldForEmbeddedCall(int index) {
        return "$" + (index + 1);
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
            collector.addDraft(lineDraft(parsedStatement, context, "", TargetObjectType.CURSOR, cursor, "OPEN",
                    RelationshipType.CURSOR_READ, lineNo, line, baseNaturalOrder));
            return true;
        }
        Matcher close = CLOSE_CURSOR_PATTERN.matcher(line);
        if (close.find()) {
            String cursor = close.group(1).trim();
            collector.addDraft(lineDraft(parsedStatement, context, "", TargetObjectType.CURSOR, cursor, "CLOSE",
                    RelationshipType.CURSOR_READ, lineNo, line, baseNaturalOrder));
            return true;
        }
        Matcher fetch = FETCH_CURSOR_PATTERN.matcher(line);
        if (!fetch.find()) {
            return false;
        }
        String cursor = fetch.group(1).trim();
        String owningRoutine = ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice());
        collector.addDraft(lineDraft(parsedStatement, context, "", TargetObjectType.CURSOR, cursor, "FETCH",
                RelationshipType.CURSOR_READ, lineNo, line, baseNaturalOrder));
        List<String> targets = splitArgs(fetch.group(2));
        List<String> selectSources = resolveCursorSelectSources(cursor, parsedStatement);
        for (int i = 0; i < targets.size(); i++) {
            String sourceField = i < selectSources.size() ? selectSources.get(i) : cursor;
            collector.addDraft(lineDraft(parsedStatement, context, sourceField, TargetObjectType.VARIABLE, owningRoutine, targets.get(i).trim(),
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
        String owningRoutine = ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice());
        collector.addDraft(lineDraft(parsedStatement, context,
                "CONSTANT:" + diagnostics.group(2).trim().toUpperCase(Locale.ROOT),
                TargetObjectType.VARIABLE,
                owningRoutine,
                diagnostics.group(1).trim(),
                RelationshipType.DIAGNOSTICS_FETCH_MAP,
                lineNo,
                line,
                baseNaturalOrder));
        return true;
    }

    private static boolean extractDiagnosticTokenAssignment(String line, int lineNo, ParsedStatementResult parsedStatement,
                                                            ExtractionContext context, RowCollector collector, int baseNaturalOrder) {
        Matcher diagnostics = DIAGNOSTIC_TOKEN_ASSIGNMENT.matcher(line);
        if (!diagnostics.find()) {
            return false;
        }
        String owningRoutine = ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice());
        collector.addDraft(lineDraft(parsedStatement, context,
                "CONSTANT:" + diagnostics.group(2).trim().toUpperCase(Locale.ROOT),
                TargetObjectType.VARIABLE,
                owningRoutine,
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
        String owningRoutine = ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice());
        collector.addDraft(lineDraft(parsedStatement, context,
                "CONSTANT:" + special.group(2).trim().toUpperCase(Locale.ROOT),
                TargetObjectType.VARIABLE,
                owningRoutine,
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
        String owningRoutine = ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice());
        String condition = handler.group(1).trim();
        condition = condition.replaceAll("(?i)\\s+SET\\b.*$", "");
        condition = condition.replaceAll("(?i)\\s+BEGIN\\b.*$", "");
        condition = condition.replaceAll(";$", "");
        condition = condition.replaceAll("\\s+", " ");
        collector.addDraft(lineDraft(parsedStatement, context,
                "CONSTANT:" + condition.toUpperCase(Locale.ROOT),
                ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice())
                        == com.example.db2lineage.model.SourceObjectType.FUNCTION
                        ? TargetObjectType.FUNCTION
                        : TargetObjectType.PROCEDURE,
                owningRoutine,
                "",
                RelationshipType.EXCEPTION_HANDLER_MAP,
                lineNo,
                line,
                baseNaturalOrder));
        return true;
    }

    private static boolean extractDeclareGlobalTemporaryTable(String line,
                                                              int lineNo,
                                                              ParsedStatementResult parsedStatement,
                                                              ExtractionContext context,
                                                              RowCollector collector,
                                                              int baseNaturalOrder) {
        Matcher dgtt = DECLARE_GLOBAL_TEMPORARY_TABLE.matcher(line);
        if (!dgtt.find()) {
            return false;
        }
        collector.addDraft(lineDraft(parsedStatement, context, "", TargetObjectType.SESSION_TABLE, dgtt.group(1).trim(), "",
                RelationshipType.CREATE_TABLE, lineNo, line, baseNaturalOrder));
        return true;
    }

    private static boolean extractDynamicSql(String line, int lineNo, ParsedStatementResult parsedStatement,
                                             ExtractionContext context, RowCollector collector, int baseNaturalOrder) {
        Matcher dynamic = EXECUTE_IMMEDIATE_PATTERN.matcher(line);
        if (!dynamic.find()) {
            return false;
        }
        collector.addDraft(lineDraft(parsedStatement, context,
                normalizeSourceToken(dynamic.group(1).trim()),
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
        return extractSetAssignment(statementText, targetObject, parsedStatement, context, collector, naturalOrder, -1, -1);
    }

    static boolean extractSetAssignment(String statementText,
                                        String targetObject,
                                        ParsedStatementResult parsedStatement,
                                        ExtractionContext context,
                                        RowCollector collector,
                                        int naturalOrder,
                                        int startLineNo,
                                        int endLineNo) {
        Pattern setAssignment = Pattern.compile("(?is)^\\s*(?:DECLARE\\s+CONTINUE\\s+HANDLER\\s+FOR\\s+.+?\\s+)?SET\\s+([A-Z0-9_.$]+)\\s*=\\s*(.+?)\\s*;?\\s*$");
        String normalized = statementText == null ? "" : statementText.replaceAll("(?m)^\\s*--.*$", "").trim();
        Matcher setMatcher = setAssignment.matcher(normalized);
        if (!setMatcher.find()) {
            return false;
        }
        String variable = setMatcher.group(1);
        String expressionText = setMatcher.group(2);
        if (isDiagnosticsStateToken(expressionText)) {
            return true;
        }
        try {
            Expression expression = CCJSqlParserUtil.parseExpression(expressionText);
            addVariableSetRowsFromExpression(
                    expression,
                    variable,
                    targetObject,
                    parsedStatement,
                    context,
                    collector,
                    naturalOrder,
                    startLineNo,
                    endLineNo,
                    shouldEmitCompanionUsageRows(expression)
            );
            return true;
        } catch (JSQLParserException ignored) {
            return false;
        }
    }

    private static boolean extractDeclareVariable(String line,
                                                  int lineNo,
                                                  ParsedStatementResult parsedStatement,
                                                  ExtractionContext context,
                                                  RowCollector collector,
                                                  int baseNaturalOrder) {
        if (!isPlainVariableDeclaration(line)) {
            return false;
        }
        Matcher declare = DECLARE_VARIABLE_WITH_OPTIONAL_DEFAULT.matcher(line);
        if (!declare.matches()) {
            return false;
        }
        String variable = declare.group(1).trim();
        String defaultExpression = declare.group(2);
        String owningRoutine = ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice());
        if (defaultExpression == null || defaultExpression.isBlank()) {
            return false;
        }

        try {
            Expression expression = CCJSqlParserUtil.parseExpression(defaultExpression.trim());
            addVariableSetRowsFromExpression(
                    expression,
                    variable,
                    owningRoutine,
                    parsedStatement,
                    context,
                    collector,
                    baseNaturalOrder,
                    lineNo,
                    lineNo,
                    false
            );
            return true;
        } catch (JSQLParserException ignored) {
            collector.addDraft(lineDraft(
                    parsedStatement,
                    context,
                    normalizeSourceToken(defaultExpression.trim()),
                    TargetObjectType.VARIABLE,
                    owningRoutine,
                    variable,
                    RelationshipType.VARIABLE_SET_MAP,
                    lineNo,
                    line,
                    baseNaturalOrder
            ));
            return true;
        }
    }

    private static void addVariableSetRowsFromExpression(Expression expression,
                                                         String targetVariable,
                                                         String targetObject,
                                                         ParsedStatementResult parsedStatement,
                                                         ExtractionContext context,
                                                         RowCollector collector,
                                                         int baseNaturalOrder,
                                                         int startLineNo,
                                                         int endLineNo,
                                                         boolean emitCompanionUsageRows) {
        List<ExpressionTokenSupport.TokenUse> tokens = (startLineNo > 0 && endLineNo > 0)
                ? ExpressionTokenSupport.collect(expression, parsedStatement.slice(), startLineNo, endLineNo)
                : ExpressionTokenSupport.collect(expression, parsedStatement.slice());
        tokens = tokens.stream()
                .filter(token -> token != null && token.token() != null && !token.token().startsWith("FUNCTION:"))
                .toList();
        tokens = normalizeVariableSetTokenOrder(tokens);
        int i = 0;
        for (ExpressionTokenSupport.TokenUse token : tokens) {
            if (emitCompanionUsageRows) {
                addSelectExpressionTokenRow(
                        token,
                        parsedStatement,
                        context,
                        collector,
                        baseNaturalOrder + i,
                        new OwnershipResolution(Map.of(), null, ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()))
                );
            }
            collector.addDraft(new RowDraft(
                    ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                    ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                    token.token(),
                    TargetObjectType.VARIABLE,
                    ObjectRelationshipSupport.normalizeObjectName(targetObject),
                    targetVariable,
                    RelationshipType.VARIABLE_SET_MAP,
                    token.lineNo(),
                    token.lineContent(),
                    ConfidenceLevel.PARSER,
                    ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                    baseNaturalOrder + i
            ));
            i++;
        }
    }

    private static List<ExpressionTokenSupport.TokenUse> normalizeVariableSetTokenOrder(List<ExpressionTokenSupport.TokenUse> tokens) {
        if (tokens == null || tokens.size() < 2) {
            return tokens == null ? List.of() : tokens;
        }
        java.util.Map<Integer, List<ExpressionTokenSupport.TokenUse>> byLine = new java.util.LinkedHashMap<>();
        for (ExpressionTokenSupport.TokenUse token : tokens) {
            byLine.computeIfAbsent(token.lineNo(), k -> new ArrayList<>()).add(token);
        }
        List<ExpressionTokenSupport.TokenUse> normalized = new ArrayList<>();
        for (List<ExpressionTokenSupport.TokenUse> lineTokens : byLine.values()) {
            if (lineTokens.size() == 2) {
                ExpressionTokenSupport.TokenUse left = lineTokens.get(0);
                ExpressionTokenSupport.TokenUse right = lineTokens.get(1);
                boolean oneConstantOneVariable =
                        (left.token().startsWith("CONSTANT:") && looksLikeVariable(right.token()))
                                || (right.token().startsWith("CONSTANT:") && looksLikeVariable(left.token()));
                if (oneConstantOneVariable && !left.token().startsWith("CONSTANT:")) {
                    normalized.add(right);
                    normalized.add(left);
                    continue;
                }
            }
            if (lineTokens.size() == 3) {
                List<ExpressionTokenSupport.TokenUse> constants = lineTokens.stream()
                        .filter(t -> t.token().startsWith("CONSTANT:"))
                        .toList();
                List<ExpressionTokenSupport.TokenUse> variables = lineTokens.stream()
                        .filter(t -> !t.token().startsWith("CONSTANT:"))
                        .toList();
                if (constants.size() == 2 && variables.size() == 1) {
                    ExpressionTokenSupport.TokenUse emptyConstant = constants.stream()
                            .filter(t -> "CONSTANT:''".equalsIgnoreCase(t.token()))
                            .findFirst()
                            .orElse(null);
                    ExpressionTokenSupport.TokenUse nonEmptyConstant = constants.stream()
                            .filter(t -> !"CONSTANT:''".equalsIgnoreCase(t.token()))
                            .findFirst()
                            .orElse(null);
                    if (emptyConstant != null && nonEmptyConstant != null) {
                        boolean commaPrefixedLabel = nonEmptyConstant.token().startsWith("CONSTANT:',");
                        if (commaPrefixedLabel) {
                            normalized.add(emptyConstant);
                            normalized.add(nonEmptyConstant);
                            normalized.add(variables.get(0));
                        } else {
                            normalized.add(nonEmptyConstant);
                            normalized.add(emptyConstant);
                            normalized.add(variables.get(0));
                        }
                        continue;
                    }
                }
            }
            normalized.addAll(lineTokens);
        }
        return List.copyOf(normalized);
    }

    private static boolean isDiagnosticsStateToken(String expressionText) {
        if (expressionText == null) {
            return false;
        }
        String normalized = expressionText.trim().toUpperCase(Locale.ROOT);
        return "SQLSTATE".equals(normalized) || "SQLCODE".equals(normalized);
    }

    private static boolean shouldEmitCompanionUsageRows(Expression expression) {
        return expression instanceof BinaryExpression
                || expression instanceof Function
                || expression instanceof InExpression
                || expression instanceof IsNullExpression
                || expression instanceof AndExpression
                || expression instanceof OrExpression
                || expression instanceof LikeExpression
                || expression instanceof EqualsTo;
    }

    private static List<String> resolveCursorSelectSources(String cursorName,
                                                           ParsedStatementResult parsedStatement) {
        List<String> lines = parsedStatement.slice().sourceFile().rawLines();
        for (int i = 0; i < lines.size(); i++) {
            Matcher declare = DECLARE_CURSOR_PATTERN.matcher(lines.get(i));
            if (!declare.find() || !declare.group(1).trim().equalsIgnoreCase(cursorName)) {
                continue;
            }
            StringBuilder statement = new StringBuilder(lines.get(i));
            int line = i + 1;
            while (!statement.toString().contains(";") && line < lines.size()) {
                statement.append('\n').append(lines.get(line));
                line++;
            }

            String statementText = statement.toString();
            Matcher forMatcher = Pattern.compile("(?is)\\bFOR\\b").matcher(statementText);
            if (!forMatcher.find()) {
                return List.of();
            }
            String selectText = statementText.substring(forMatcher.end()).trim();
            if (selectText.endsWith(";")) {
                selectText = selectText.substring(0, selectText.length() - 1);
            }
            try {
                var parsed = CCJSqlParserUtil.parse(selectText);
                if (!(parsed instanceof Select select) || select.getPlainSelect() == null) {
                    return conservativeCursorColumnFallback(selectText);
                }
                PlainSelect plainSelect = select.getPlainSelect();
                List<String> tokens = new ArrayList<>();
                for (SelectItem<?> selectItem : plainSelect.getSelectItems()) {
                    Expression expression = selectItem.getExpression();
                    if (expression != null) {
                        if (expression instanceof net.sf.jsqlparser.schema.Column column) {
                            tokens.add(column.getColumnName());
                        } else {
                            tokens.add(normalizeSourceToken(expression.toString()));
                        }
                    }
                }
                if (tokens.isEmpty()) {
                    return conservativeCursorColumnFallback(selectText);
                }
                return List.copyOf(tokens);
            } catch (Exception ignored) {
                return conservativeCursorColumnFallback(selectText);
            }
        }
        return List.of();
    }

    private static List<String> conservativeCursorColumnFallback(String selectText) {
        if (selectText == null || selectText.isBlank()) {
            return List.of();
        }
        String upper = selectText.toUpperCase(Locale.ROOT);
        int selectIdx = upper.indexOf("SELECT");
        if (selectIdx < 0) {
            return List.of();
        }
        Matcher fromMatcher = Pattern.compile("(?is)\\bFROM\\b").matcher(selectText);
        if (!fromMatcher.find(selectIdx + "SELECT".length())) {
            return List.of();
        }
        int fromIdx = fromMatcher.start();
        String selectList = selectText.substring(selectIdx + "SELECT".length(), fromIdx);
        List<String> items = splitArgs(selectList);
        List<String> result = new ArrayList<>();
        for (String raw : items) {
            String item = raw.trim();
            if (item.isEmpty()) {
                return List.of();
            }
            String normalized = item.replaceAll("(?i)\\s+AS\\s+.+$", "").trim();
            int dot = normalized.lastIndexOf('.');
            String token = dot >= 0 ? normalized.substring(dot + 1).trim() : normalized;
            if (!token.matches("[A-Z0-9_.$]+")) {
                return List.of();
            }
            result.add(token);
        }
        return List.copyOf(result);
    }

    private static int findExpressionLine(StatementSlice slice, String expressionText, int startLine) {
        if (expressionText == null || expressionText.isBlank()) {
            return -1;
        }
        String normalizedExpression = expressionText.replaceAll("\\s+", " ").trim().toUpperCase(Locale.ROOT);
        int boundedStart = Math.max(slice.startLine(), startLine);
        for (int line = boundedStart; line <= slice.endLine(); line++) {
            String normalizedLine = slice.sourceFile().getRawLine(line).replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
            if (normalizedLine.contains(normalizedExpression)) {
                return line;
            }
        }
        return -1;
    }

    private static List<Integer> findInsertValueLines(StatementSlice slice) {
        List<Integer> lines = new ArrayList<>();
        boolean sawValues = false;
        boolean inTuple = false;
        for (int line = slice.startLine(); line <= slice.endLine(); line++) {
            String raw = slice.sourceFile().getRawLine(line);
            String trimmed = raw.trim();
            String upper = trimmed.toUpperCase(Locale.ROOT);
            if (!sawValues) {
                if (upper.startsWith("VALUES") || upper.contains(" VALUES")) {
                    sawValues = true;
                }
                continue;
            }
            if (!inTuple) {
                if (trimmed.startsWith("(")) {
                    inTuple = true;
                }
                continue;
            }
            if (trimmed.startsWith(")") || trimmed.startsWith(");")) {
                break;
            }
            if (trimmed.isBlank() || trimmed.startsWith("--")) {
                continue;
            }
            lines.add(line);
        }
        return List.copyOf(lines);
    }

    private static void addCompanionSelectExprRowsForInsertValues(Expression expression,
                                                                  ParsedStatementResult parsedStatement,
                                                                  ExtractionContext context,
                                                                  RowCollector collector,
                                                                  int baseNaturalOrder,
                                                                  int valueLine) {
        if (expression == null) {
            return;
        }
        int startLine = valueLine > 0 ? valueLine : parsedStatement.slice().startLine();
        int endLine = valueLine > 0 ? valueLine : parsedStatement.slice().endLine();
        List<ExpressionTokenSupport.TokenUse> tokens = MappingRelationshipSupport.conciseMappingTokens(
                expression,
                parsedStatement.slice(),
                RelationshipType.INSERT_SELECT_MAP,
                startLine,
                endLine
        );
        for (ExpressionTokenSupport.TokenUse token : tokens) {
            addSelectExpressionTokenRow(
                    token,
                    parsedStatement,
                    context,
                    collector,
                    baseNaturalOrder,
                    new OwnershipResolution(Map.of(), null, ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()))
            );
        }
    }

    private static void addSelectFieldRow(Column column,
                                          ParsedStatementResult parsedStatement,
                                          ExtractionContext context,
                                          RowCollector collector,
                                          int baseNaturalOrder,
                                          OwnershipResolution resolution,
                                          int preferredLine) {
        if (column == null) {
            return;
        }
        String sourceField = column.getColumnName();
        if (sourceField == null || sourceField.isBlank()) {
            sourceField = normalizeSourceToken(column.getFullyQualifiedName());
        }
        String anchorToken = column.getFullyQualifiedName();
        if (anchorToken == null || anchorToken.isBlank()) {
            anchorToken = sourceField;
        }
        LineAnchorResolver.LineAnchor anchor;
        if (preferredLine > 0) {
            String rawLine = parsedStatement.slice().sourceFile().getRawLine(preferredLine);
            int idx = rawLine.toUpperCase(Locale.ROOT).indexOf(anchorToken.toUpperCase(Locale.ROOT));
            anchor = new LineAnchorResolver.LineAnchor(preferredLine, rawLine, Math.max(0, idx), idx >= 0);
        } else {
            anchor = LineAnchorResolver.token(parsedStatement.slice(), anchorToken, baseNaturalOrder);
        }

        TableOwner owner = null;
        if (column.getTable() != null && column.getTable().getName() != null) {
            owner = resolution.resolve(column.getTable().getName());
        }
        if (owner == null) {
            owner = resolution.singleOwner().orElse(null);
        }
        TargetObjectType targetType = owner == null ? TargetObjectType.UNKNOWN : owner.type();
        String targetObject = owner == null ? ObjectRelationshipSupport.UNKNOWN_UNRESOLVED_OBJECT : owner.objectName();

        collector.addDraft(new RowDraft(
                ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                sourceField,
                targetType,
                targetObject,
                sourceField,
                RelationshipType.SELECT_FIELD,
                anchor.lineNo(),
                anchor.lineContent(),
                ConfidenceLevel.PARSER,
                ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                baseNaturalOrder
        ));
    }

    private static int addSelectExpressionRows(Expression expression,
                                               ParsedStatementResult parsedStatement,
                                               ExtractionContext context,
                                               RowCollector collector,
                                               int baseNaturalOrder,
                                               OwnershipResolution resolution,
                                               int searchStartLine) {
        if (expression == null) {
            return searchStartLine;
        }
        int boundedStartLine = Math.max(parsedStatement.slice().startLine(), searchStartLine);
        List<ExpressionTokenSupport.TokenUse> tokens;
        if (expression instanceof CaseExpression caseExpression) {
            tokens = collectCaseExpressionTokens(caseExpression, parsedStatement.slice(), boundedStartLine);
        } else {
            tokens = ExpressionTokenSupport.collect(expression, parsedStatement.slice(), boundedStartLine, parsedStatement.slice().endLine());
        }
        java.util.Map<Integer, Integer> perLineOrder = new java.util.HashMap<>();
        int maxLine = boundedStartLine;
        for (ExpressionTokenSupport.TokenUse token : tokens) {
            if (token == null || token.token() == null || token.token().startsWith("FUNCTION:")) {
                continue;
            }
            int orderOnLine = perLineOrder.getOrDefault(token.lineNo(), 0);
            addSelectExpressionTokenRow(token, parsedStatement, context, collector, baseNaturalOrder, resolution, orderOnLine);
            perLineOrder.put(token.lineNo(), orderOnLine + 1);
            maxLine = Math.max(maxLine, token.lineNo());
        }
        return maxLine;
    }

    private static List<ExpressionTokenSupport.TokenUse> collectCaseExpressionTokens(CaseExpression caseExpression,
                                                                                      StatementSlice slice,
                                                                                      int searchStartLine) {
        if (caseExpression == null) {
            return List.of();
        }
        List<ExpressionTokenSupport.TokenUse> tokens = new ArrayList<>();
        int searchLine = Math.max(slice.startLine(), searchStartLine);
        if (caseExpression.getSwitchExpression() != null) {
            searchLine = collectExpressionWithProgress(caseExpression.getSwitchExpression(), slice, searchLine, tokens);
        }
        if (caseExpression.getWhenClauses() != null) {
            for (var whenClause : caseExpression.getWhenClauses()) {
                if (whenClause == null) {
                    continue;
                }
                if (whenClause.getWhenExpression() != null) {
                    searchLine = collectExpressionWithProgress(whenClause.getWhenExpression(), slice, searchLine, tokens);
                }
                if (whenClause.getThenExpression() != null) {
                    searchLine = collectExpressionWithProgress(whenClause.getThenExpression(), slice, searchLine, tokens);
                }
            }
        }
        if (caseExpression.getElseExpression() != null) {
            collectExpressionWithProgress(caseExpression.getElseExpression(), slice, searchLine, tokens);
        }
        return List.copyOf(tokens);
    }

    private static int collectExpressionWithProgress(Expression expression,
                                                     StatementSlice slice,
                                                     int searchLine,
                                                     List<ExpressionTokenSupport.TokenUse> sink) {
        if (expression == null) {
            return searchLine;
        }
        int expressionLine = findExpressionLine(slice, expression.toString(), searchLine);
        if (expressionLine < 0) {
            expressionLine = searchLine;
        }
        sink.addAll(ExpressionTokenSupport.collect(expression, slice, expressionLine, slice.endLine()));
        return Math.max(searchLine, expressionLine);
    }

    private static void addSelectExpressionTokenRow(ExpressionTokenSupport.TokenUse tokenUse,
                                                    ParsedStatementResult parsedStatement,
                                                    ExtractionContext context,
                                                    RowCollector collector,
                                                    int baseNaturalOrder,
                                                    OwnershipResolution resolution) {
        if (tokenUse == null || tokenUse.token() == null || tokenUse.token().startsWith("FUNCTION:")) {
            return;
        }
        addSelectExpressionTokenRow(tokenUse, parsedStatement, context, collector, baseNaturalOrder, resolution, tokenUse.orderOnLine());
    }

    private static void addSelectExpressionTokenRow(ExpressionTokenSupport.TokenUse tokenUse,
                                                    ParsedStatementResult parsedStatement,
                                                    ExtractionContext context,
                                                    RowCollector collector,
                                                    int baseNaturalOrder,
                                                    OwnershipResolution resolution,
                                                    int orderOnLine) {
        if (tokenUse == null || tokenUse.token() == null || tokenUse.token().startsWith("FUNCTION:")) {
            return;
        }
        SelectExprTarget target = resolveSelectExprToken(tokenUse.token(), resolution);
        collector.addDraft(new RowDraft(
                ObjectRelationshipSupport.sourceObjectType(parsedStatement.slice()),
                ObjectRelationshipSupport.sourceObjectName(parsedStatement.slice()),
                target.sourceField(),
                target.targetType(),
                target.targetObject(),
                target.targetField(),
                RelationshipType.SELECT_EXPR,
                tokenUse.lineNo(),
                tokenUse.lineContent(),
                ConfidenceLevel.PARSER,
                ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                baseNaturalOrder + orderOnLine
        ));
    }

    private static SelectExprTarget resolveSelectExprToken(String token, OwnershipResolution resolution) {
        if (token == null || token.isBlank()) {
            return new SelectExprTarget("", TargetObjectType.UNKNOWN, "UNKNOWN_SELECT_EXPR", "");
        }
        String normalized = normalizeSourceToken(token);
        if (normalized.startsWith("CONSTANT:") || normalized.startsWith("SEQUENCE:")) {
            return new SelectExprTarget(normalized, TargetObjectType.UNKNOWN, "UNKNOWN_SELECT_EXPR", "");
        }

        int dot = normalized.lastIndexOf('.');
        if (dot > 0 && dot < normalized.length() - 1) {
            String qualifier = normalized.substring(0, dot);
            String field = normalized.substring(dot + 1);
            TableOwner owner = resolution.resolve(qualifier);
            if (owner != null) {
                return new SelectExprTarget(field, owner.type(), owner.objectName(), field);
            }
            return new SelectExprTarget(field, TargetObjectType.UNKNOWN, "UNKNOWN_SELECT_EXPR", field);
        }

        String lower = normalized.toLowerCase(Locale.ROOT);
        if (lower.startsWith("lv_")
                || lower.startsWith("ld_")
                || lower.startsWith("ln_")
                || lower.startsWith("cv_")
                || lower.startsWith("p_")
                || "at_end".equals(lower)) {
            return new SelectExprTarget(normalized, TargetObjectType.VARIABLE, resolution.routineName(), normalized);
        }
        if (resolution.singleOwner().isPresent()) {
            TableOwner owner = resolution.singleOwner().get();
            return new SelectExprTarget(normalized, owner.type(), owner.objectName(), normalized);
        }
        return new SelectExprTarget(normalized, TargetObjectType.UNKNOWN, "UNKNOWN_SELECT_EXPR", normalized);
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
                ConfidenceLevel.REGEX,
                ObjectRelationshipSupport.statementOrder(context, parsedStatement),
                naturalOrder
        );
    }

    private static RowDraft parserLineDraft(ParsedStatementResult parsedStatement,
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
