package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.model.ConfidenceLevel;
import com.example.db2lineage.model.RelationshipRow;
import com.example.db2lineage.model.RelationshipType;
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
    void doesNotTreatStringLiteralContainingCallAsProcedureInvocation() {
        SqlSourceFile sourceFile = sqlFile("call_literal.sql", List.of(
                "'Start CALL RPT.PR_TEST_DEMO()'"
        ), SqlSourceCategory.SP_DIR);

        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(slice(sourceFile, 1, sourceFile.rawLines().get(0), 0));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.CALL_PROCEDURE));
    }

    @Test
    void doesNotTreatCommentContainingCallAsProcedureInvocation() {
        SqlSourceFile sourceFile = sqlFile("commented_call.sql", List.of(
                "-- CALL TEMP.PR_PROCEDURE_LOG('INFO');"
        ), SqlSourceCategory.SP_DIR);

        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(slice(sourceFile, 1, sourceFile.rawLines().get(0), 0));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.CALL_PROCEDURE));
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
