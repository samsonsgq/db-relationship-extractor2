package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.model.RelationshipRow;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.model.ConfidenceLevel;
import com.example.db2lineage.parse.ParsedStatementResult;
import com.example.db2lineage.parse.SqlSourceCategory;
import com.example.db2lineage.parse.SqlSourceFile;
import com.example.db2lineage.parse.SqlStatementParser;
import com.example.db2lineage.parse.StatementSlicer;
import com.example.db2lineage.resolve.InMemorySchemaMetadataService;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrTestDemoRegressionTest {

    @Test
    void prTestDemoProceduralRowsFollowContractAndAvoidFalsePositives() throws Exception {
        Path sqlPath = Path.of("src/main/java/com/example/db2lineage/resources/sample_case/sp/RPT.PR_TEST_DEMO.sql");
        List<String> rawLines = Files.readAllLines(sqlPath);
        SqlSourceFile sourceFile = new SqlSourceFile(
                SqlSourceCategory.SP_DIR,
                sqlPath.toAbsolutePath(),
                Path.of("RPT.PR_TEST_DEMO.sql"),
                String.join("\n", rawLines),
                rawLines
        );

        StatementSlicer slicer = new StatementSlicer();
        SqlStatementParser parser = new SqlStatementParser();
        List<ParsedStatementResult> parsed = slicer.slice(sourceFile).stream().map(parser::parse).toList();
        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(parsed)),
                parsed
        );

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CREATE_PROCEDURE
                && "RPT.PR_TEST_DEMO".equals(r.targetObject())));

        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.CONTROL_FLOW_CONDITION
                && r.lineContent().contains("RESULT SETS")));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.UNKNOWN
                && r.lineContent().trim().startsWith("DECLARE lv_")));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.CONTROL_FLOW_CONDITION
                && r.lineContent().trim().startsWith("DECLARE")));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.CURSOR_READ
                && r.lineContent().trim().endsWith(":")));

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CREATE_TABLE
                && r.targetObjectType() == TargetObjectType.SESSION_TABLE
                && "SESSION.TMP_STO_EVENT_SOURCE".equals(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.DIAGNOSTICS_FETCH_MAP
                && "CONSTANT:MESSAGE_TEXT".equals(r.sourceField())
                && "lv_message_text".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.DIAGNOSTICS_FETCH_MAP
                && "CONSTANT:ROW_COUNT".equals(r.sourceField())
                && "lv_row_count".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.DIAGNOSTICS_FETCH_MAP
                && "CONSTANT:SQLCODE".equals(r.sourceField())
                && "ln_sqlcode".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.DIAGNOSTICS_FETCH_MAP
                && "CONSTANT:SQLSTATE".equals(r.sourceField())
                && "lv_sqlstate".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.EXCEPTION_HANDLER_MAP
                && "CONSTANT:SQLEXCEPTION".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.EXCEPTION_HANDLER_MAP
                && "CONSTANT:NOT FOUND".equals(r.sourceField())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.CALL_PROCEDURE
                && "RPT.PR_TEST_DEMO".equals(r.targetObject())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.UNKNOWN
                && (r.lineContent().trim().equals("COMMIT;") || r.lineContent().trim().equals("ROLLBACK;"))));

        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.CREATE_TABLE
                && "SESSION.TMP_STO_EVENT_SOURCE".equals(r.targetObject())
                && r.lineNo() == 68));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.EXCEPTION_HANDLER_MAP
                && "CONSTANT:SQLEXCEPTION".equals(r.sourceField())
                && r.lineNo() == 100));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.DIAGNOSTICS_FETCH_MAP
                && "CONSTANT:SQLCODE".equals(r.sourceField())
                && "ln_sqlcode".equalsIgnoreCase(r.targetField())
                && r.lineNo() == 102));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.DIAGNOSTICS_FETCH_MAP
                && "CONSTANT:MESSAGE_TEXT".equals(r.sourceField())
                && "lv_message_text".equalsIgnoreCase(r.targetField())
                && r.lineNo() == 102));

        RelationshipRow bizSelect = rows.stream()
                .filter(r -> r.relationship() == RelationshipType.SELECT_FIELD
                        && r.lineNo() == 178
                        && "BIZ_DT".equalsIgnoreCase(r.sourceField()))
                .min(Comparator.comparingInt(RelationshipRow::lineRelationSeq))
                .orElseThrow();
        RelationshipRow bizMap = rows.stream()
                .filter(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                        && r.lineNo() == 178
                        && "BIZ_DT".equalsIgnoreCase(r.sourceField())
                        && "ld_biz_date".equalsIgnoreCase(r.targetField()))
                .min(Comparator.comparingInt(RelationshipRow::lineRelationSeq))
                .orElseThrow();
        assertTrue(bizSelect.lineRelationSeq() < bizMap.lineRelationSeq());

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_TABLE
                && r.lineNo() == 188
                && "TEMP.SYS_WHERE".equalsIgnoreCase(r.targetObject())
                && r.confidence() == ConfidenceLevel.REGEX));

        assertEquals(1, rows.stream()
                .filter(r -> r.relationship() == RelationshipType.CALL_FUNCTION
                        && "TEMP.FN_GET_ACTUAL_MONTH_BEGIN_DATE".equalsIgnoreCase(r.targetObject())
                        && r.lineNo() == 191)
                .count());
        assertEquals(1, rows.stream()
                .filter(r -> r.relationship() == RelationshipType.FUNCTION_EXPR_MAP
                        && "ld_actual_month_begin_date".equalsIgnoreCase(r.targetField())
                        && r.lineNo() == 191)
                .count());
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.CALL_FUNCTION
                && "TEMP.FN_GET_ACTUAL_MONTH_BEGIN_DATE".equalsIgnoreCase(r.targetObject())
                && r.lineNo() == 192
                && "    SET ld_actual_month_begin_date = TEMP.FN_GET_ACTUAL_MONTH_BEGIN_DATE();".equals(r.lineContent())));

        List<RelationshipRow> line196 = rows.stream()
                .filter(r -> r.lineNo() == 196)
                .sorted(Comparator.comparingInt(RelationshipRow::lineRelationSeq))
                .toList();
        assertTrue(line196.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && "lv_month_flag".equalsIgnoreCase(r.sourceField())));
        assertTrue(line196.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && "CONSTANT:'E'".equalsIgnoreCase(r.sourceField())
                && r.targetObjectType() == TargetObjectType.UNKNOWN
                && "UNKNOWN_SELECT_EXPR".equalsIgnoreCase(r.targetObject())));
        assertTrue(line196.stream().anyMatch(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                && "ld_actual_month_end_date".equalsIgnoreCase(r.sourceField())
                && "ld_end_date".equalsIgnoreCase(r.targetField())));
        assertFalse(line196.stream().anyMatch(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                && "lv_month_flag".equalsIgnoreCase(r.sourceField())));
        assertFalse(line196.stream().anyMatch(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                && "CONSTANT:'E'".equalsIgnoreCase(r.sourceField())));
        assertFalse(line196.stream().anyMatch(r -> r.relationship() == RelationshipType.CONTROL_FLOW_CONDITION));

        List<RelationshipRow> line287 = rows.stream()
                .filter(r -> r.lineNo() == 287)
                .sorted(Comparator.comparingInt(RelationshipRow::lineRelationSeq))
                .toList();
        assertEquals(1, line287.stream()
                .filter(r -> r.relationship() == RelationshipType.SELECT_EXPR
                        && "CONSTANT:'N'".equalsIgnoreCase(r.sourceField()))
                .count());
        assertEquals(1, line287.stream()
                .filter(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP
                        && "CONSTANT:'EXPIRY'".equalsIgnoreCase(r.sourceField())
                        && "SESSION.TMP_STO_EVENT_SOURCE".equalsIgnoreCase(r.targetObject())
                        && "EVENT_CLASS".equalsIgnoreCase(r.targetField()))
                .count());
        assertEquals(2, line287.stream()
                .filter(r -> r.relationship() == RelationshipType.FUNCTION_EXPR_MAP
                        && "COALESCE".equalsIgnoreCase(r.sourceField())
                        && "SESSION.TMP_STO_EVENT_SOURCE".equalsIgnoreCase(r.targetObject()))
                .count());
    }
}
