package com.example.db2lineage.parse;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SqlStatementParserTest {

    private final SqlStatementParser parser = new SqlStatementParser();

    @Test
    void parsesInsertSelect() {
        StatementSlice slice = slice(
                SqlSourceCategory.TABLE_DIR,
                "insert_select.sql",
                10,
                List.of("INSERT INTO T1 (ID) SELECT S.ID FROM S")
        );

        ParsedStatementResult result = parser.parse(slice);

        assertFalse(result.parseFailed());
        assertFalse(result.unsupported());
        assertTrue(result.statement().isPresent());
        assertInstanceOf(Insert.class, result.statement().get());
        assertTrue(result.parseIssues().isEmpty());
        assertSame(slice, result.slice());
    }

    @Test
    void parsesUpdateSet() {
        StatementSlice slice = slice(
                SqlSourceCategory.SP_DIR,
                "update_set.sql",
                2,
                List.of("UPDATE T1 SET NAME = 'X' WHERE ID = 1")
        );

        ParsedStatementResult result = parser.parse(slice);

        assertFalse(result.parseFailed());
        assertFalse(result.unsupported());
        assertTrue(result.statement().isPresent());
        assertInstanceOf(Update.class, result.statement().get());
    }

    @Test
    void parsesSimpleSelect() {
        StatementSlice slice = slice(
                SqlSourceCategory.VIEW_DIR,
                "simple_select.sql",
                1,
                List.of("SELECT ID FROM T1")
        );

        ParsedStatementResult result = parser.parse(slice);

        assertFalse(result.parseFailed());
        assertFalse(result.unsupported());
        assertTrue(result.statement().isPresent());
        Statement statement = result.statement().get();
        assertInstanceOf(Select.class, statement);
    }

    @Test
    void parsesCreateView() {
        StatementSlice slice = slice(
                SqlSourceCategory.VIEW_DIR,
                "create_view.sql",
                1,
                List.of("CREATE VIEW V1 AS SELECT ID FROM T1")
        );

        ParsedStatementResult result = parser.parse(slice);

        assertFalse(result.parseFailed());
        assertFalse(result.unsupported());
        assertTrue(result.statement().isPresent());
        assertInstanceOf(CreateView.class, result.statement().get());
    }

    @Test
    void returnsParseFailedWithIssuesAndPreservesSliceMetadata() {
        StatementSlice slice = slice(
                SqlSourceCategory.FUNCTION_DIR,
                "broken.sql",
                20,
                List.of("SELECT FROM")
        );

        ParsedStatementResult result = parser.parse(slice);

        assertTrue(result.parseFailed());
        assertFalse(result.unsupported());
        assertTrue(result.statement().isEmpty());
        assertFalse(result.parseIssues().isEmpty());
        assertEquals("JSQLPARSER_ERROR", result.parseIssues().get(0).code());

        StatementSlice returnedSlice = result.slice();
        assertSame(slice, returnedSlice);
        assertEquals(SqlSourceCategory.FUNCTION_DIR, returnedSlice.sourceCategory());
        assertEquals(20, returnedSlice.startLine());
        assertEquals(20, returnedSlice.endLine());
        assertEquals("SELECT FROM", returnedSlice.statementText());
        assertEquals(List.of("SELECT FROM"), returnedSlice.rawLines());
        assertEquals(0, returnedSlice.ordinalWithinFile());
        assertEquals(Path.of("broken.sql"), returnedSlice.sourceFile().relativePath());
    }

    @Test
    void marksUnsupportedDb2ProceduralStatement() {
        StatementSlice slice = slice(
                SqlSourceCategory.SP_DIR,
                "unsupported.sql",
                30,
                List.of("GET DIAGNOSTICS V_SQLSTATE = RETURNED_SQLSTATE")
        );

        ParsedStatementResult result = parser.parse(slice);

        assertFalse(result.parseFailed());
        assertTrue(result.unsupported());
        assertTrue(result.statement().isEmpty());
        assertEquals(2, result.parseIssues().size());
        assertEquals("UNSUPPORTED_DB2_PROCEDURAL_SYNTAX", result.parseIssues().get(1).code());
    }

    private StatementSlice slice(
            SqlSourceCategory sourceCategory,
            String filename,
            int startLine,
            List<String> rawLines
    ) {
        SqlSourceFile sourceFile = new SqlSourceFile(
                sourceCategory,
                Path.of("/tmp/" + filename),
                Path.of(filename),
                String.join("\n", rawLines),
                rawLines
        );
        return new StatementSlice(
                sourceFile,
                sourceCategory,
                String.join("\n", rawLines),
                startLine,
                startLine + rawLines.size() - 1,
                rawLines,
                0
        );
    }
}
