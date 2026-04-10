package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.model.RelationshipRow;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.parse.ParsedStatementResult;
import com.example.db2lineage.parse.SqlSourceCategory;
import com.example.db2lineage.parse.SqlSourceFile;
import com.example.db2lineage.parse.SqlStatementParser;
import com.example.db2lineage.parse.StatementSlice;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExtractionPipelineTest {

    @Test
    void extractReturnsUnknownForUnsupportedProceduralStatement() {
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

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.UNKNOWN));
    }

    @Test
    void extractObjectLevelRowsForSelectInsertUpdateDeleteMergeAndTruncate() {
        SqlSourceFile sourceFile = new SqlSourceFile(
                SqlSourceCategory.SP_DIR,
                Path.of("/tmp/object_level.sql"),
                Path.of("object_level.sql"),
                String.join("\n", List.of(
                        "WITH C AS (SELECT ID FROM T_SRC) SELECT * FROM C UNION ALL SELECT ID FROM T2;",
                        "INSERT INTO T_DST (ID) SELECT ID FROM T_SRC;",
                        "UPDATE T_DST D SET ID = 2 FROM T_SRC S;",
                        "DELETE FROM T_DST WHERE ID IN (SELECT ID FROM T_SRC);",
                        "MERGE INTO T_DST D USING T_SRC S ON D.ID = S.ID WHEN MATCHED THEN UPDATE SET ID = S.ID;",
                        "TRUNCATE TABLE T_DST;"
                )),
                List.of(
                        "WITH C AS (SELECT ID FROM T_SRC) SELECT * FROM C UNION ALL SELECT ID FROM T2;",
                        "INSERT INTO T_DST (ID) SELECT ID FROM T_SRC;",
                        "UPDATE T_DST D SET ID = 2 FROM T_SRC S;",
                        "DELETE FROM T_DST WHERE ID IN (SELECT ID FROM T_SRC);",
                        "MERGE INTO T_DST D USING T_SRC S ON D.ID = S.ID WHEN MATCHED THEN UPDATE SET ID = S.ID;",
                        "TRUNCATE TABLE T_DST;"
                )
        );

        SqlStatementParser parser = new SqlStatementParser();
        List<ParsedStatementResult> parsed = List.of(
                parser.parse(slice(sourceFile, 1, "WITH C AS (SELECT ID FROM T_SRC) SELECT * FROM C UNION ALL SELECT ID FROM T2;", 0)),
                parser.parse(slice(sourceFile, 2, "INSERT INTO T_DST (ID) SELECT ID FROM T_SRC;", 1)),
                parser.parse(slice(sourceFile, 3, "UPDATE T_DST D SET ID = 2 FROM T_SRC S;", 2)),
                parser.parse(slice(sourceFile, 4, "DELETE FROM T_DST WHERE ID IN (SELECT ID FROM T_SRC);", 3)),
                parser.parse(slice(sourceFile, 5, "MERGE INTO T_DST D USING T_SRC S ON D.ID = S.ID WHEN MATCHED THEN UPDATE SET ID = S.ID;", 4)),
                parser.parse(slice(sourceFile, 6, "TRUNCATE TABLE T_DST;", 5))
        );

        List<RelationshipRow> rows = new ExtractionPipeline().extract(new ExtractionContext(List.of(sourceFile)), parsed);
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CTE_DEFINE));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CTE_READ));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.UNION_INPUT));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_TABLE));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.UPDATE_TABLE));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.DELETE_TABLE));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.MERGE_INTO));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.TRUNCATE_TABLE));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.UPDATE_SET));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.MERGE_MATCH));
    }

    @Test
    void extractCreateAndDynamicSqlRowsWithStableUnknownTargets() {
        SqlSourceFile sourceFile = new SqlSourceFile(
                SqlSourceCategory.FUNCTION_DIR,
                Path.of("/tmp/create_and_dynamic.sql"),
                Path.of("create_and_dynamic.sql"),
                String.join("\n", List.of(
                        "CREATE VIEW V1 AS SELECT ID FROM T1;",
                        "CREATE TABLE T1 (ID INT);",
                        "CREATE FUNCTION F1() RETURNS INT RETURN 1;",
                        "CREATE PROCEDURE P1() LANGUAGE SQL BEGIN SELECT 1; END",
                        "GET DIAGNOSTICS V_SQLSTATE = RETURNED_SQLSTATE"
                )),
                List.of(
                        "CREATE VIEW V1 AS SELECT ID FROM T1;",
                        "CREATE TABLE T1 (ID INT);",
                        "CREATE FUNCTION F1() RETURNS INT RETURN 1;",
                        "CREATE PROCEDURE P1() LANGUAGE SQL BEGIN SELECT 1; END",
                        "GET DIAGNOSTICS V_SQLSTATE = RETURNED_SQLSTATE"
                )
        );

        SqlStatementParser parser = new SqlStatementParser();
        List<ParsedStatementResult> parsed = List.of(
                parser.parse(slice(sourceFile, 1, "CREATE VIEW V1 AS SELECT ID FROM T1;", 0)),
                parser.parse(slice(sourceFile, 2, "CREATE TABLE T1 (ID INT);", 1)),
                parser.parse(slice(sourceFile, 3, "CREATE FUNCTION F1() RETURNS INT RETURN 1;", 2)),
                parser.parse(slice(sourceFile, 4, "CREATE PROCEDURE P1() LANGUAGE SQL BEGIN SELECT 1; END", 3)),
                parser.parse(slice(sourceFile, 5, "GET DIAGNOSTICS V_SQLSTATE = RETURNED_SQLSTATE", 4))
        );

        List<RelationshipRow> rows = new ExtractionPipeline().extract(new ExtractionContext(List.of(sourceFile)), parsed);
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CREATE_VIEW));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CREATE_TABLE));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CREATE_FUNCTION));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CREATE_PROCEDURE));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.UNKNOWN));
        assertTrue(rows.stream().anyMatch(r ->
                r.relationship() == RelationshipType.UNKNOWN
                        && "UNKNOWN_UNSUPPORTED_STATEMENT".equals(r.targetObject())));
    }

    @Test
    void extractDynamicSqlExecFromFallbackStatementText() {
        SqlSourceFile sourceFile = new SqlSourceFile(
                SqlSourceCategory.SP_DIR,
                Path.of("/tmp/dynamic.sql"),
                Path.of("dynamic.sql"),
                "EXECUTE IMMEDIATE V_SQL;",
                List.of("EXECUTE IMMEDIATE V_SQL;")
        );

        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(slice(sourceFile, 1, "EXECUTE IMMEDIATE V_SQL;", 0));
        List<RelationshipRow> rows = new ExtractionPipeline().extract(new ExtractionContext(List.of(sourceFile)), List.of(parsed));

        assertEquals(1, rows.size());
        assertEquals(RelationshipType.DYNAMIC_SQL_EXEC, rows.get(0).relationship());
        assertEquals("UNKNOWN_DYNAMIC_SQL", rows.get(0).targetObject());
        assertEquals("V_SQL", rows.get(0).sourceField());
    }


    @Test
    void extractSelectFieldAndExpressionAndClauses() {
        SqlSourceFile sourceFile = new SqlSourceFile(
                SqlSourceCategory.VIEW_DIR,
                Path.of("/tmp/field_usage.sql"),
                Path.of("field_usage.sql"),
                "SELECT T.ID, T.AMT + 1 AS AMT2 FROM T1 T JOIN T2 X ON T.ID = X.ID WHERE T.FLAG = 'Y' GROUP BY T.ID ORDER BY T.ID;",
                List.of("SELECT T.ID, T.AMT + 1 AS AMT2 FROM T1 T JOIN T2 X ON T.ID = X.ID WHERE T.FLAG = 'Y' GROUP BY T.ID ORDER BY T.ID;")
        );

        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(slice(sourceFile, 1,
                "SELECT T.ID, T.AMT + 1 AS AMT2 FROM T1 T JOIN T2 X ON T.ID = X.ID WHERE T.FLAG = 'Y' GROUP BY T.ID ORDER BY T.ID;", 0));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(new ExtractionContext(List.of(sourceFile)), List.of(parsed));

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_FIELD && r.sourceField().equals("T.ID")));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR && r.sourceField().equals("T.AMT")));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR && r.sourceField().equals("CONSTANT:1")));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE && r.sourceField().equals("CONSTANT:'Y'")));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.JOIN_ON && r.sourceField().equals("X.ID")));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.GROUP_BY && r.sourceField().equals("T.ID")));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.ORDER_BY && r.sourceField().equals("T.ID")));
    }

    @Test
    void extractScalarFunctionReturnExpressionDependencies() {
        SqlSourceFile sourceFile = new SqlSourceFile(
                SqlSourceCategory.FUNCTION_DIR,
                Path.of("/tmp/return_expr.sql"),
                Path.of("return_expr.sql"),
                "CREATE FUNCTION F_RET(P1 INT) RETURNS INT RETURN P1 + 5;",
                List.of("CREATE FUNCTION F_RET(P1 INT) RETURNS INT RETURN P1 + 5;")
        );

        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(slice(sourceFile, 1,
                "CREATE FUNCTION F_RET(P1 INT) RETURNS INT RETURN P1 + 5;", 0));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(new ExtractionContext(List.of(sourceFile)), List.of(parsed));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.RETURN_VALUE && r.sourceField().equals("P1")));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.RETURN_VALUE && r.sourceField().equals("CONSTANT:5")));
    }

    private StatementSlice slice(SqlSourceFile sourceFile, int line, String sql, int ordinal) {
        return new StatementSlice(
                sourceFile,
                sourceFile.sourceCategory(),
                sql,
                line,
                line,
                List.of(sql),
                ordinal
        );
    }
}
