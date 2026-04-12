package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.model.RelationshipRow;
import com.example.db2lineage.model.RelationshipType;
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

import static org.junit.jupiter.api.Assertions.assertTrue;

class PrTestDemoProceduralLineageTest {

    @Test
    void proceduralMappingsAndOrderingArePresentForPrTestDemo() throws Exception {
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
        ).stream().filter(r -> "RPT.PR_TEST_DEMO".equals(r.sourceObject())).toList();

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.DIAGNOSTICS_FETCH_MAP
                && r.lineNo() == 101
                && "CONSTANT:MESSAGE_TEXT".equals(r.sourceField())
                && "lv_message_text".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.DIAGNOSTICS_FETCH_MAP
                && r.lineNo() == 102
                && "CONSTANT:ROW_COUNT".equals(r.sourceField())
                && "lv_row_count".equalsIgnoreCase(r.targetField())));

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SPECIAL_REGISTER_MAP
                && r.lineNo() == 98
                && "CONSTANT:SQLCODE".equals(r.sourceField())
                && "ln_sqlcode".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SPECIAL_REGISTER_MAP
                && r.lineNo() == 99
                && "CONSTANT:SQLSTATE".equals(r.sourceField())
                && "lv_sqlstate".equalsIgnoreCase(r.targetField())));

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CURSOR_READ
                && r.lineNo() == 494
                && "c_event_detail".equalsIgnoreCase(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CURSOR_READ
                && r.lineNo() == 747
                && "c_event_detail".equalsIgnoreCase(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CURSOR_FETCH_MAP
                && "cv_customer_number".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CURSOR_FETCH_MAP
                && "cv_knock_out_flag".equalsIgnoreCase(r.targetField())));

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP
                && r.lineNo() == 167
                && "$1".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP
                && r.lineNo() == 170
                && "$4".equalsIgnoreCase(r.targetField())));

    }
}
