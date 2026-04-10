package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.model.RelationshipRow;
import com.example.db2lineage.parse.ParsedStatementResult;
import com.example.db2lineage.parse.SqlSourceCategory;
import com.example.db2lineage.parse.SqlSourceFile;
import com.example.db2lineage.parse.SqlStatementParser;
import com.example.db2lineage.parse.StatementSlice;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExtractionPipelineTest {

    @Test
    void extractDispatchesThroughConfiguredExtractorsAndReturnsFinalizedRows() {
        SqlSourceFile sourceFile = new SqlSourceFile(
                SqlSourceCategory.VIEW_DIR,
                Path.of("/tmp/sample.sql"),
                Path.of("sample.sql"),
                "SELECT 1;\nGET DIAGNOSTICS v = 1;",
                List.of("SELECT 1;", "GET DIAGNOSTICS v = 1;")
        );

        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult select = parser.parse(new StatementSlice(
                sourceFile,
                SqlSourceCategory.VIEW_DIR,
                "SELECT 1;",
                1,
                1,
                List.of("SELECT 1;"),
                0
        ));
        ParsedStatementResult diagnostics = parser.parse(new StatementSlice(
                sourceFile,
                SqlSourceCategory.VIEW_DIR,
                "GET DIAGNOSTICS v = 1;",
                2,
                2,
                List.of("GET DIAGNOSTICS v = 1;"),
                1
        ));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile)),
                List.of(select, diagnostics)
        );

        assertEquals(0, rows.size());
    }
}
