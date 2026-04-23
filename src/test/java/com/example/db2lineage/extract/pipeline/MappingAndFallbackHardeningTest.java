package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.model.ConfidenceLevel;
import com.example.db2lineage.model.RelationshipRow;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.parse.ParseIssue;
import com.example.db2lineage.parse.ParsedStatementResult;
import com.example.db2lineage.parse.SqlSourceCategory;
import com.example.db2lineage.parse.SqlSourceFile;
import com.example.db2lineage.parse.SqlStatementParser;
import com.example.db2lineage.parse.StatementSlice;
import com.example.db2lineage.resolve.InMemorySchemaMetadataService;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MappingAndFallbackHardeningTest {

    @Test
    void updateSetMapWithFunctionCallEmitsFunctionExprMapWithoutArgumentToTargetMapping() {
        SqlSourceFile sourceFile = sqlFile("func_map.sql", List.of(
                "UPDATE T_DST SET SCORE = FN_SCORE(SRC_VAL);"
        ), SqlSourceCategory.SP_DIR);

        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(slice(sourceFile, 1, sourceFile.rawLines().get(0), 0));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.FUNCTION_EXPR_MAP
                && "SCORE".equals(r.targetField())
                && "FN_SCORE".equals(r.sourceField())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.UPDATE_SET_MAP
                && "SCORE".equals(r.targetField())
                && "SRC_VAL".equals(r.sourceField())));
    }

    @Test
    void functionExprMapUsesOnlyOutermostFunctionForTargetSlot() {
        SqlSourceFile sourceFile = sqlFile("outermost_only.sql", List.of(
                "UPDATE T_DST SET SCORE = COALESCE(CHAR(SRC_VAL), '0');"
        ), SqlSourceCategory.SP_DIR);

        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(slice(sourceFile, 1, sourceFile.rawLines().get(0), 0));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        long functionExprMapCount = rows.stream()
                .filter(r -> r.relationship() == RelationshipType.FUNCTION_EXPR_MAP && "SCORE".equals(r.targetField()))
                .count();
        assertEquals(1, functionExprMapCount);
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.FUNCTION_EXPR_MAP
                && "SCORE".equals(r.targetField())
                && "COALESCE".equals(r.sourceField())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.FUNCTION_EXPR_MAP
                && "SCORE".equals(r.targetField())
                && "CHAR".equals(r.sourceField())));
    }

    @Test
    void functionExprMapNotEmittedWhenExpressionRootIsNotFunction() {
        SqlSourceFile sourceFile = sqlFile("case_root.sql", List.of(
                "UPDATE T_DST SET SCORE = CASE WHEN COALESCE(SRC_VAL, 0) > 0 THEN 1 ELSE 0 END;"
        ), SqlSourceCategory.SP_DIR);

        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(slice(sourceFile, 1, sourceFile.rawLines().get(0), 0));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.FUNCTION_EXPR_MAP
                && "SCORE".equals(r.targetField())));
    }

    @Test
    void proceduralRegexRowsUseRegexConfidence() {
        SqlSourceFile sourceFile = sqlFile("diag.sql", List.of(
                "GET DIAGNOSTICS V_SQLSTATE = RETURNED_SQLSTATE"
        ), SqlSourceCategory.SP_DIR);

        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(slice(sourceFile, 1, sourceFile.rawLines().get(0), 0));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.DIAGNOSTICS_FETCH_MAP
                && r.confidence() == ConfidenceLevel.REGEX));
    }

    @Test
    void executeImmediatePreservesWholeSourceExpressionToken() {
        SqlSourceFile sourceFile = sqlFile("dynamic.sql", List.of(
                "EXECUTE IMMEDIATE V_SQL || ' WHERE X = 1';"
        ), SqlSourceCategory.SP_DIR);

        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(slice(sourceFile, 1, sourceFile.rawLines().get(0), 0));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        assertEquals(1, rows.size());
        RelationshipRow row = rows.get(0);
        assertEquals(RelationshipType.DYNAMIC_SQL_EXEC, row.relationship());
        assertEquals("V_SQL || ' WHERE X = 1'", row.sourceField());
    }

    @Test
    void functionAssignmentEmitsCallFunctionAndAnchorsRowsToExactLine() {
        SqlSourceFile sourceFile = sqlFile("fn_assign.sql", List.of(
                "CREATE PROCEDURE RPT.PR_TEST_DEMO()",
                "BEGIN",
                "    SET ld_actual_month_begin_date = TEMP.FN_GET_ACTUAL_MONTH_BEGIN_DATE();",
                "    SET ld_actual_month_end_date   = TEMP.FN_GET_ACTUAL_MONTH_END_DATE();",
                "END"
        ), SqlSourceCategory.SP_DIR);

        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(new StatementSlice(
                sourceFile,
                sourceFile.sourceCategory(),
                String.join("\n", sourceFile.rawLines()),
                1,
                sourceFile.rawLines().size(),
                sourceFile.rawLines(),
                0
        ));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_FUNCTION
                && "TEMP.FN_GET_ACTUAL_MONTH_BEGIN_DATE".equals(r.targetObject())
                && r.lineNo() == 3
                && "    SET ld_actual_month_begin_date = TEMP.FN_GET_ACTUAL_MONTH_BEGIN_DATE();".equals(r.lineContent())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_FUNCTION
                && "TEMP.FN_GET_ACTUAL_MONTH_END_DATE".equals(r.targetObject())
                && r.lineNo() == 4
                && "    SET ld_actual_month_end_date   = TEMP.FN_GET_ACTUAL_MONTH_END_DATE();".equals(r.lineContent())));

        long beginMappings = rows.stream()
                .filter(r -> r.relationship() == RelationshipType.FUNCTION_EXPR_MAP
                        && "ld_actual_month_begin_date".equals(r.targetField()))
                .count();
        assertEquals(1, beginMappings);
    }

    @Test
    void parseFailedProceduralChunkSplitsPhysicalLinesForFunctionAssignments() {
        String first = "    SET ld_actual_month_begin_date = TEMP.FN_GET_ACTUAL_MONTH_BEGIN_DATE();";
        String second = "    SET ld_actual_month_end_date   = TEMP.FN_GET_ACTUAL_MONTH_END_DATE();";
        SqlSourceFile sourceFile = sqlFile("fn_assign_chunk.sql", List.of(
                first + "\n" + second
        ), SqlSourceCategory.SP_DIR);

        StatementSlice slice = new StatementSlice(
                sourceFile,
                sourceFile.sourceCategory(),
                first + "\n" + second,
                191,
                192,
                sourceFile.rawLines(),
                0
        );
        ParsedStatementResult parsed = ParsedStatementResult.parseFailed(slice, List.<ParseIssue>of());

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of())),
                List.of(parsed)
        );

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_FUNCTION
                && "TEMP.FN_GET_ACTUAL_MONTH_BEGIN_DATE".equals(r.targetObject())
                && r.lineNo() == 191
                && first.equals(r.lineContent())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_FUNCTION
                && "TEMP.FN_GET_ACTUAL_MONTH_END_DATE".equals(r.targetObject())
                && r.lineNo() == 192
                && second.equals(r.lineContent())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.FUNCTION_EXPR_MAP
                && "ld_actual_month_begin_date".equals(r.targetField())
                && r.lineNo() == 192));
    }

    private SqlSourceFile sqlFile(String name, List<String> rawLines, SqlSourceCategory category) {
        return new SqlSourceFile(
                category,
                Path.of("/tmp/" + name),
                Path.of(name),
                String.join("\n", rawLines),
                rawLines
        );
    }

    private StatementSlice slice(SqlSourceFile sourceFile, int lineNo, String text, int ordinal) {
        return new StatementSlice(
                sourceFile,
                sourceFile.sourceCategory(),
                text,
                lineNo,
                lineNo,
                List.of(text),
                ordinal
        );
    }
}
