package com.example.db2lineage.parse;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StatementSlicerTest {

    private final StatementSlicer slicer = new StatementSlicer();

    @Test
    void returnsSingleStatementWithExactRangeAndRawLines() {
        SqlSourceFile file = sourceFile(
                SqlSourceCategory.TABLE_DIR,
                "single.sql",
                List.of(
                        "CREATE TABLE T1 (ID INT);",
                        "",
                        "-- trailing comment"
                )
        );

        List<StatementSlice> slices = slicer.slice(file);

        assertEquals(1, slices.size());
        StatementSlice slice = slices.get(0);
        assertEquals(file, slice.sourceFile());
        assertEquals(SqlSourceCategory.TABLE_DIR, slice.sourceCategory());
        assertEquals(1, slice.startLine());
        assertEquals(3, slice.endLine());
        assertEquals(List.of("CREATE TABLE T1 (ID INT);", "", "-- trailing comment"), slice.rawLines());
        assertEquals("CREATE TABLE T1 (ID INT);\n\n-- trailing comment", slice.statementText());
        assertEquals(0, slice.ordinalWithinFile());
    }

    @Test
    void splitsMultipleStatementsUsingAtOnOwnLineOnly() {
        SqlSourceFile file = sourceFile(
                SqlSourceCategory.VIEW_DIR,
                "many.sql",
                List.of(
                        "CREATE VIEW V1 AS SELECT 1 AS X",
                        "@",
                        "",
                        "CREATE VIEW V2 AS SELECT 2 AS Y",
                        "   @   ",
                        "CREATE VIEW V3 AS SELECT 3 AS Z"
                )
        );

        List<StatementSlice> slices = slicer.slice(file);

        assertEquals(3, slices.size());

        assertEquals(1, slices.get(0).startLine());
        assertEquals(1, slices.get(0).endLine());
        assertEquals(0, slices.get(0).ordinalWithinFile());

        assertEquals(4, slices.get(1).startLine());
        assertEquals(4, slices.get(1).endLine());
        assertEquals(List.of("CREATE VIEW V2 AS SELECT 2 AS Y"), slices.get(1).rawLines());
        assertEquals(1, slices.get(1).ordinalWithinFile());

        assertEquals(6, slices.get(2).startLine());
        assertEquals(6, slices.get(2).endLine());
        assertEquals(2, slices.get(2).ordinalWithinFile());
    }

    @Test
    void skipsLeadingBlankLinesAfterDelimiterWhenAnchoringStatementStart() {
        SqlSourceFile file = sourceFile(
                SqlSourceCategory.EXTRA_DIR,
                "blanks.sql",
                List.of(
                        "SELECT 1",
                        "@",
                        "",
                        "",
                        "EXECUTE IMMEDIATE 'DELETE FROM T1'",
                        "@"
                )
        );

        List<StatementSlice> slices = slicer.slice(file);

        assertEquals(2, slices.size());
        assertEquals(5, slices.get(1).startLine());
        assertEquals(List.of("EXECUTE IMMEDIATE 'DELETE FROM T1'"), slices.get(1).rawLines());
    }

    @Test
    void doesNotSplitInsideStringLiteralsOrCommentsAndPreservesCategory() {
        SqlSourceFile file = sourceFile(
                SqlSourceCategory.FUNCTION_DIR,
                "routine.sql",
                List.of(
                        "VALUES '@';",
                        "-- @",
                        "/*",
                        "@",
                        "*/",
                        "VALUES 2;",
                        "@",
                        "VALUES 'done';"
                )
        );

        List<StatementSlice> slices = slicer.slice(file);

        assertEquals(2, slices.size());
        StatementSlice first = slices.get(0);
        assertEquals(SqlSourceCategory.FUNCTION_DIR, first.sourceCategory());
        assertEquals(1, first.startLine());
        assertEquals(6, first.endLine());
        assertEquals(List.of(
                "VALUES '@';",
                "-- @",
                "/*",
                "@",
                "*/",
                "VALUES 2;"
        ), first.rawLines());

        StatementSlice second = slices.get(1);
        assertEquals(8, second.startLine());
        assertEquals(8, second.endLine());
        assertEquals("VALUES 'done';", second.statementText());
        assertEquals(1, second.ordinalWithinFile());
    }

    @Test
    void doesNotSplitWhenAtAppearsInsideBlockCommentSameLine() {
        SqlSourceFile file = sourceFile(
                SqlSourceCategory.SP_DIR,
                "comment.sql",
                List.of(
                        "/* @ */",
                        "VALUES 1;"
                )
        );

        List<StatementSlice> slices = slicer.slice(file);

        assertEquals(1, slices.size());
        assertEquals(1, slices.get(0).startLine());
        assertEquals(2, slices.get(0).endLine());
    }

    private SqlSourceFile sourceFile(SqlSourceCategory category, String name, List<String> rawLines) {
        String fullText = String.join("\n", rawLines) + "\n";
        return new SqlSourceFile(category, Path.of("/tmp/" + name), Path.of(name), fullText, rawLines);
    }
}
