package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.model.RelationshipRow;
import com.example.db2lineage.model.RelationshipType;
import com.example.db2lineage.model.TargetObjectType;
import com.example.db2lineage.parse.ParsedStatementResult;
import com.example.db2lineage.parse.SqlSourceCategory;
import com.example.db2lineage.parse.SqlSourceFile;
import com.example.db2lineage.parse.SqlStatementParser;
import com.example.db2lineage.parse.StatementSlice;
import com.example.db2lineage.parse.StatementSlicer;
import com.example.db2lineage.resolve.InMemorySchemaMetadataService;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(select, diagnostics))),
                List.of(select, diagnostics)
        );

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.DIAGNOSTICS_FETCH_MAP));
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

        List<RelationshipRow> rows = new ExtractionPipeline().extract(new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(parsed)), parsed);
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CTE_DEFINE));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CTE_READ));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.UNION_INPUT));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_TABLE), rows.toString());
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

        List<RelationshipRow> rows = new ExtractionPipeline().extract(new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(parsed)), parsed);
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CREATE_VIEW));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CREATE_TABLE));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CREATE_FUNCTION));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CREATE_PROCEDURE));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.DIAGNOSTICS_FETCH_MAP));
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
        List<RelationshipRow> rows = new ExtractionPipeline().extract(new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))), List.of(parsed));

        assertEquals(1, rows.size());
        assertEquals(RelationshipType.DYNAMIC_SQL_EXEC, rows.get(0).relationship());
        assertEquals("UNKNOWN_DYNAMIC_SQL", rows.get(0).targetObject());
        assertEquals("V_SQL", rows.get(0).sourceField());
    }

    @Test
    void extractDynamicSqlExecLiteralUsesConstantTokenAndExactExecuteLine() {
        SqlSourceFile sourceFile = new SqlSourceFile(
                SqlSourceCategory.EXTRA_DIR,
                Path.of("/tmp/dynamic_literal.sql"),
                Path.of("dynamic_literal.sql"),
                "SELECT 1;\n@\n\nEXECUTE IMMEDIATE 'DELETE FROM T1';\n@",
                List.of("SELECT 1;", "@", "", "EXECUTE IMMEDIATE 'DELETE FROM T1';", "@")
        );

        StatementSlicer slicer = new StatementSlicer();
        SqlStatementParser parser = new SqlStatementParser();
        List<ParsedStatementResult> parsed = slicer.slice(sourceFile).stream().map(parser::parse).toList();
        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(parsed)),
                parsed
        );

        RelationshipRow row = rows.stream()
                .filter(r -> r.relationship() == RelationshipType.DYNAMIC_SQL_EXEC)
                .findFirst()
                .orElseThrow();

        assertEquals("CONSTANT:'DELETE FROM T1'", row.sourceField());
        assertEquals(4, row.lineNo());
        assertEquals("EXECUTE IMMEDIATE 'DELETE FROM T1';", row.lineContent());
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

        List<RelationshipRow> rows = new ExtractionPipeline().extract(new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))), List.of(parsed));

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_FIELD
                && r.sourceField().equals("T.ID")
                && r.targetField().equals("ID")
                && r.targetObject().equals("T1")));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR && r.sourceField().equals("T.AMT")));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR && r.sourceField().equals("CONSTANT:1")));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE && r.sourceField().equals("CONSTANT:'Y'")));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.JOIN_ON && r.sourceField().equals("X.ID")));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.GROUP_BY && r.sourceField().equals("T.ID")));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.ORDER_BY && r.sourceField().equals("T.ID")));
    }

    @Test
    void extractInsertSelectMappingUsesPositionalSlotsAndConciseCaseTokens() {
        SqlSourceFile sourceFile = new SqlSourceFile(
                SqlSourceCategory.SP_DIR,
                Path.of("/tmp/insert_select_map.sql"),
                Path.of("insert_select_map.sql"),
                "INSERT INTO T_DST (A, B, C) SELECT SRC_A, COALESCE(SRC_B, ''), CASE WHEN FLAG='Y' THEN SRC_C ELSE 0 END FROM T_SRC;",
                List.of("INSERT INTO T_DST (A, B, C) SELECT SRC_A, COALESCE(SRC_B, ''), CASE WHEN FLAG='Y' THEN SRC_C ELSE 0 END FROM T_SRC;")
        );

        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(slice(sourceFile, 1, sourceFile.rawLines().get(0), 0));
        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP
                && "A".equals(r.targetField()) && "SRC_A".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP
                && "B".equals(r.targetField()) && "SRC_B".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP
                && "B".equals(r.targetField()) && "CONSTANT:''".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP
                && "C".equals(r.targetField()) && "SRC_C".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP
                && "C".equals(r.targetField()) && "CONSTANT:0".equals(r.sourceField())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP
                && "C".equals(r.targetField()) && "FLAG".equals(r.sourceField())));
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

        List<RelationshipRow> rows = new ExtractionPipeline().extract(new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))), List.of(parsed));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.RETURN_VALUE && r.sourceField().equals("P1")));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.RETURN_VALUE && r.sourceField().equals("CONSTANT:5")));
    }

    @Test
    void extractReturnDependenciesAnchorToReturnLineAndDoNotEmitUnknownForReturn() {
        SqlSourceFile sourceFile = new SqlSourceFile(
                SqlSourceCategory.FUNCTION_DIR,
                Path.of("/tmp/get_multiplier_like.sql"),
                Path.of("get_multiplier_like.sql"),
                String.join("\n", List.of(
                        "CREATE OR REPLACE FUNCTION GET_MULTIPLIER ( P_FACTOR INT ) RETURNS INT",
                        "LANGUAGE SQL",
                        "BEGIN",
                        "    RETURN P_FACTOR * 2;",
                        "END"
                )),
                List.of(
                        "CREATE OR REPLACE FUNCTION GET_MULTIPLIER ( P_FACTOR INT ) RETURNS INT",
                        "LANGUAGE SQL",
                        "BEGIN",
                        "    RETURN P_FACTOR * 2;",
                        "END"
                )
        );

        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(new StatementSlice(
                sourceFile,
                sourceFile.sourceCategory(),
                sourceFile.fullText(),
                1,
                5,
                sourceFile.rawLines(),
                0
        ));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.RETURN_VALUE && "P_FACTOR".equals(r.sourceField()) && r.lineNo() == 4));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.RETURN_VALUE && "CONSTANT:2".equals(r.sourceField()) && r.lineNo() == 4));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.UNKNOWN && r.lineNo() == 4));
    }

    @Test
    void phase9InsertTargetColumnAndMappingRules() {
        SqlSourceFile sourceFile = sqlFile("phase9_insert.sql", List.of(
                "CREATE TABLE T_DST (A INT, B INT);",
                "INSERT INTO T_DST (A, B) SELECT S.X, COALESCE(S.Y, 0) FROM T_SRC S;",
                "INSERT INTO T_DST SELECT S.X, S.Y FROM T_SRC S;"
        ), SqlSourceCategory.TABLE_DIR);
        SqlStatementParser parser = new SqlStatementParser();
        List<ParsedStatementResult> parsed = List.of(
                parser.parse(slice(sourceFile, 1, sourceFile.rawLines().get(0), 0)),
                parser.parse(slice(sourceFile, 2, sourceFile.rawLines().get(1), 1)),
                parser.parse(slice(sourceFile, 3, sourceFile.rawLines().get(2), 2))
        );
        List<RelationshipRow> rows = new ExtractionPipeline().extract(new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(parsed)), parsed);
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_TARGET_COL && "A".equals(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_TARGET_COL && "B".equals(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP && "A".equals(r.targetField()) && "S.X".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP && "B".equals(r.targetField()) && "S.Y".equals(r.sourceField())));
    }

    @Test
    void phase9UpdateMergeViewAndConciseExpressionMappings() {
        SqlSourceFile sourceFile = sqlFile("phase9_mix.sql", List.of(
                "CREATE VIEW V1 AS SELECT S.ID, CASE WHEN S.FLAG = 'Y' THEN COALESCE(S.AMT, 0) ELSE 1 END AS AMT FROM SRC S;",
                "UPDATE T_DST SET A = S.X, B = CASE WHEN S.F = 1 THEN COALESCE(S.Y, 0) ELSE S.Z END FROM T_SRC S;",
                "MERGE INTO T_DST D USING T_SRC S ON D.ID = S.ID WHEN MATCHED THEN UPDATE SET A = S.X, B = S.Y WHEN NOT MATCHED THEN INSERT (A, B) VALUES (S.X, COALESCE(S.Y, 0));"
        ), SqlSourceCategory.VIEW_DIR);
        SqlStatementParser parser = new SqlStatementParser();
        List<ParsedStatementResult> parsed = List.of(
                parser.parse(slice(sourceFile, 1, sourceFile.rawLines().get(0), 0)),
                parser.parse(slice(sourceFile, 2, sourceFile.rawLines().get(1), 1)),
                parser.parse(slice(sourceFile, 3, sourceFile.rawLines().get(2), 2))
        );
        List<RelationshipRow> rows = new ExtractionPipeline().extract(new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(parsed)), parsed);
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CREATE_VIEW_MAP && "AMT".equals(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.UPDATE_TARGET_COL && "A".equals(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.UPDATE_SET_MAP && "B".equals(r.targetField()) && "S.Z".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.MERGE_TARGET_COL && "A".equals(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.MERGE_SET_MAP && "B".equals(r.targetField()) && "S.Y".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.FUNCTION_EXPR_MAP && "B".equals(r.targetField()) && "COALESCE".equals(r.sourceField())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.UPDATE_SET_MAP && r.sourceField().startsWith("FUNCTION:")));
    }

    @Test
    void phase10RoutineSpecificMappingsAndFallbacks() {
        SqlSourceFile sourceFile = sqlFile("phase10_proc.sql", List.of(
                "CREATE PROCEDURE P_LOAD(IN P_ID INT, INOUT P_STATUS VARCHAR(20), OUT P_TS TIMESTAMP)",
                "BEGIN",
                "  DECLARE C_DEAL CURSOR FOR SELECT DEAL_NUM FROM T_DEAL;",
                "  OPEN C_DEAL;",
                "  FETCH C_DEAL INTO V_REC.DEAL_NUM, V_STATUS;",
                "  CALL P_AUDIT(P_ID, V_STATUS, P_TS);",
                "  SET V_USER = USER;",
                "  SET V_DT = CURRENT DATE;",
                "  SET V_TS = CURRENT TIMESTAMP;",
                "  GET DIAGNOSTICS V_SQLSTATE = RETURNED_SQLSTATE;",
                "  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET V_ERR = 1;",
                "  EXECUTE IMMEDIATE V_SQL;",
                "END"
        ), SqlSourceCategory.SP_DIR);
        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(new StatementSlice(
                sourceFile,
                sourceFile.sourceCategory(),
                sourceFile.fullText(),
                1,
                sourceFile.getLineCount(),
                sourceFile.rawLines(),
                0
        ));
        List<ParsedStatementResult> metadataParsed = List.of(
                parsed,
                parser.parse(slice(sourceFile, 14, "CREATE PROCEDURE P_AUDIT(IN A_ID INT, INOUT A_STATUS VARCHAR(20), OUT A_TS TIMESTAMP)", 1))
        );
        List<RelationshipRow> rows = new ExtractionPipeline().extract(new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(metadataParsed)), List.of(parsed));

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CURSOR_DEFINE && "C_DEAL".equals(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CURSOR_READ && "C_DEAL".equals(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CURSOR_FETCH_MAP
                && "P_LOAD".equals(r.targetObject())
                && "V_REC.DEAL_NUM".equals(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP && "A_ID".equals(r.targetField()) && "P_ID".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP && "A_STATUS".equals(r.targetField()) && "V_STATUS".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP && "A_TS".equals(r.targetField()) && "P_TS".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SPECIAL_REGISTER_MAP
                && "P_LOAD".equals(r.targetObject())
                && "CONSTANT:USER".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SPECIAL_REGISTER_MAP
                && "P_LOAD".equals(r.targetObject())
                && "CONSTANT:CURRENT DATE".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SPECIAL_REGISTER_MAP
                && "P_LOAD".equals(r.targetObject())
                && "CONSTANT:CURRENT TIMESTAMP".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.DIAGNOSTICS_FETCH_MAP
                && "P_LOAD".equals(r.targetObject())
                && "CONSTANT:RETURNED_SQLSTATE".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.EXCEPTION_HANDLER_MAP
                && "P_LOAD".equals(r.targetObject())
                && r.targetObjectType() == TargetObjectType.PROCEDURE
                && ("EXIT".equalsIgnoreCase(r.targetField()) || "CONTINUE".equalsIgnoreCase(r.targetField()))));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.DYNAMIC_SQL_EXEC && "UNKNOWN_DYNAMIC_SQL".equals(r.targetObject())));
    }

    @Test
    void proceduralMultilineStatementsEmitSelectAndInsertTableRowsWithoutDuplicates() {
        SqlSourceFile sourceFile = sqlFile("phase11_table_rows.sql", List.of(
                "CREATE PROCEDURE P_TABLE_ROWS()",
                "BEGIN",
                "  -- insert from select",
                "  INSERT INTO SESSION.T_TMP",
                "  (ID)",
                "  SELECT SRC.ID",
                "    FROM T_SRC SRC;",
                "  -- plain select",
                "  SELECT A.COL1, A.COL2",
                "    INTO V1, V2",
                "    FROM T_A A;",
                "END"
        ), SqlSourceCategory.SP_DIR);
        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(new StatementSlice(
                sourceFile,
                sourceFile.sourceCategory(),
                sourceFile.fullText(),
                1,
                sourceFile.getLineCount(),
                sourceFile.rawLines(),
                0
        ));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        assertTrue(rows.stream()
                .filter(r -> r.relationship() == RelationshipType.INSERT_TABLE
                        && "SESSION.T_TMP".equalsIgnoreCase(r.targetObject()))
                .count() >= 1);
        assertTrue(rows.stream()
                .filter(r -> r.relationship() == RelationshipType.SELECT_TABLE
                        && "T_SRC".equalsIgnoreCase(r.targetObject()))
                .count() >= 1);
        assertTrue(rows.stream()
                .filter(r -> r.relationship() == RelationshipType.SELECT_TABLE
                        && "T_A".equalsIgnoreCase(r.targetObject()))
                .count() >= 1);
    }

    @Test
    void createProcedureExtractsNestedDmlWithExactBodyLineAnchors() {
        SqlSourceFile sourceFile = new SqlSourceFile(
                SqlSourceCategory.SP_DIR,
                Path.of("/tmp/p_nested.sql"),
                Path.of("p_nested.sql"),
                String.join("\n", List.of(
                        "CREATE OR REPLACE PROCEDURE P_NESTED()",
                        "LANGUAGE SQL",
                        "BEGIN",
                        "    INSERT INTO T_DST (ID) SELECT ID FROM T_SRC;",
                        "END",
                        "@"
                )),
                List.of(
                        "CREATE OR REPLACE PROCEDURE P_NESTED()",
                        "LANGUAGE SQL",
                        "BEGIN",
                        "    INSERT INTO T_DST (ID) SELECT ID FROM T_SRC;",
                        "END",
                        "@"
                )
        );

        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(new StatementSlice(
                sourceFile,
                sourceFile.sourceCategory(),
                String.join("\n", sourceFile.rawLines().subList(0, 5)),
                1,
                5,
                sourceFile.rawLines().subList(0, 5),
                0
        ));
        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CREATE_PROCEDURE && r.sourceObject().equals("P_NESTED")));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_TABLE));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_TARGET_COL && "ID".equals(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP && "ID".equals(r.targetField()) && "ID".equals(r.sourceField())));
    }

    @Test
    void createViewRowsUseCanonicalObjectNameAsSourceObject() {
        SqlSourceFile sourceFile = sqlFile("INTERFACE.V1.sql", List.of(
                "CREATE VIEW INTERFACE.V1 AS SELECT T1.ID AS ID FROM T1;"
        ), SqlSourceCategory.VIEW_DIR);
        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(slice(sourceFile, 1, sourceFile.rawLines().get(0), 0));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        assertTrue(rows.stream().allMatch(r -> r.sourceObject().equals("INTERFACE.V1")));
    }

    @Test
    void phase10FunctionAndProcedureParamFallbackToPositionalSlotsWhenUnknown() {
        SqlSourceFile sourceFile = sqlFile("phase10_fn.sql", List.of(
                "SET V_OUT = FN_SCORE(P_ID, CURRENT DATE, 1);",
                "CALL P_LOG(V_A, V_B);"
        ), SqlSourceCategory.EXTRA_DIR);
        SqlStatementParser parser = new SqlStatementParser();
        List<ParsedStatementResult> parsed = List.of(
                parser.parse(slice(sourceFile, 1, sourceFile.rawLines().get(0), 0)),
                parser.parse(slice(sourceFile, 2, sourceFile.rawLines().get(1), 1))
        );
        List<RelationshipRow> rows = new ExtractionPipeline().extract(new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(parsed)), parsed);

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.FUNCTION_PARAM_MAP && "FN_SCORE".equals(r.targetObject()) && "$1".equals(r.targetField()) && "P_ID".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.FUNCTION_PARAM_MAP && "FN_SCORE".equals(r.targetObject()) && "$2".equals(r.targetField()) && "CONSTANT:CURRENT DATE".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.FUNCTION_PARAM_MAP && "FN_SCORE".equals(r.targetObject()) && "$3".equals(r.targetField()) && "CONSTANT:1".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.FUNCTION_EXPR_MAP
                && "PHASE10_FN".equals(r.targetObject())
                && "V_OUT".equals(r.targetField())
                && "FN_SCORE".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP && "P_LOG".equals(r.targetObject()) && "$1".equals(r.targetField().trim())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP && "P_LOG".equals(r.targetObject()) && "$2".equals(r.targetField().trim())));
    }

    @Test
    void scriptSourceObjectFallbackUsesUppercaseFilenameStem() {
        SqlSourceFile sourceFile = sqlFile("extra_patterns.sql", List.of(
                "SELECT 1 FROM SYSIBM.SYSDUMMY1;"
        ), SqlSourceCategory.EXTRA_DIR);
        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(slice(sourceFile, 1, sourceFile.rawLines().get(0), 0));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        assertTrue(rows.stream().allMatch(r -> "EXTRA_PATTERNS".equals(r.sourceObject())));
    }

    @Test
    void routineNestedStatementAnchorsSkipLeadingBlankAndCommentLines() {
        SqlSourceFile sourceFile = sqlFile("p_anchor.sql", List.of(
                "CREATE OR REPLACE PROCEDURE P_ANCHOR()",
                "LANGUAGE SQL",
                "BEGIN",
                "",
                "    -- comment before statement",
                "    SET V_SQL = 'DELETE FROM T1';",
                "    EXECUTE IMMEDIATE V_SQL;",
                "END",
                "@"
        ), SqlSourceCategory.SP_DIR);

        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(new StatementSlice(
                sourceFile,
                sourceFile.sourceCategory(),
                String.join("\n", sourceFile.rawLines().subList(0, 8)),
                1,
                8,
                sourceFile.rawLines().subList(0, 8),
                0
        ));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        assertTrue(rows.stream().anyMatch(r ->
                r.relationship() == RelationshipType.DYNAMIC_SQL_EXEC
                        && r.lineNo() == 7
                        && "    EXECUTE IMMEDIATE V_SQL;".equals(r.lineContent())
        ));
    }

    @Test
    void phase12UsesViewAwareReadAndDeleteRelationshipsWhenMetadataResolvesView() {
        SqlSourceFile sourceFile = sqlFile("phase12_view_rw.sql", List.of(
                "CREATE VIEW V_SRC AS SELECT ID FROM T_SRC;",
                "SELECT ID FROM V_SRC;",
                "DELETE FROM V_SRC WHERE ID = 1;"
        ), SqlSourceCategory.VIEW_DIR);
        SqlStatementParser parser = new SqlStatementParser();
        List<ParsedStatementResult> parsed = List.of(
                parser.parse(slice(sourceFile, 1, sourceFile.rawLines().get(0), 0)),
                parser.parse(slice(sourceFile, 2, sourceFile.rawLines().get(1), 1)),
                parser.parse(slice(sourceFile, 3, sourceFile.rawLines().get(2), 2))
        );

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(parsed)),
                parsed
        );

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_VIEW && "V_SRC".equals(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.DELETE_VIEW && "V_SRC".equals(r.targetObject())));
    }

    @Test
    void phase12EmitsControlFlowConditionRowsFromRoutineLines() {
        SqlSourceFile sourceFile = sqlFile("phase12_control_flow.sql", List.of(
                "CREATE PROCEDURE P_CTRL()",
                "BEGIN",
                "  IF V_COUNT > 0 THEN",
                "    SET V_FLAG = 1;",
                "  END IF;",
                "END"
        ), SqlSourceCategory.SP_DIR);
        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(new StatementSlice(
                sourceFile,
                sourceFile.sourceCategory(),
                sourceFile.fullText(),
                1,
                sourceFile.getLineCount(),
                sourceFile.rawLines(),
                0
        ));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CONTROL_FLOW_CONDITION && "V_COUNT".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CONTROL_FLOW_CONDITION && "CONSTANT:0".equals(r.sourceField())));
    }

    @Test
    void phase12DoesNotSilentlySkipLaterProceduralLinesInUnsupportedSlices() {
        SqlSourceFile sourceFile = sqlFile("phase12_multi_line_fallback.sql", List.of(
                "GET DIAGNOSTICS V_SQLSTATE = RETURNED_SQLSTATE;",
                "EXECUTE IMMEDIATE V_SQL;"
        ), SqlSourceCategory.EXTRA_DIR);
        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(new StatementSlice(
                sourceFile,
                sourceFile.sourceCategory(),
                sourceFile.fullText(),
                1,
                sourceFile.getLineCount(),
                sourceFile.rawLines(),
                0
        ));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.DIAGNOSTICS_FETCH_MAP));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.DYNAMIC_SQL_EXEC));
    }

    @Test
    void phase13ParsesCallWithoutArgumentListAsProcedureInvocation() {
        SqlSourceFile sourceFile = sqlFile("phase13_call_no_args.sql", List.of(
                "CALL P_PING;"
        ), SqlSourceCategory.EXTRA_DIR);
        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(slice(sourceFile, 1, sourceFile.rawLines().get(0), 0));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_PROCEDURE
                && "P_PING".equals(r.targetObject())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP));
    }

    @Test
    void phase13TableFunctionPipeRowsMapToDeclaredReturnColumns() {
        SqlSourceFile sourceFile = sqlFile("phase13_table_fn.sql", List.of(
                "CREATE FUNCTION F_RET()",
                "RETURNS TABLE (C1 INT, C2 DECIMAL(10,2))",
                "LANGUAGE SQL",
                "BEGIN",
                "  PIPE (SRC.A, 1);",
                "  RETURN;",
                "END"
        ), SqlSourceCategory.FUNCTION_DIR);
        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(new StatementSlice(
                sourceFile,
                sourceFile.sourceCategory(),
                sourceFile.fullText(),
                1,
                sourceFile.getLineCount(),
                sourceFile.rawLines(),
                0
        ));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.TABLE_FUNCTION_RETURN_MAP
                && "F_RET".equals(r.targetObject())
                && "C1".equals(r.targetField())
                && "SRC.A".equals(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.TABLE_FUNCTION_RETURN_MAP
                && "F_RET".equals(r.targetObject())
                && "C2".equals(r.targetField())
                && "CONSTANT:1".equals(r.sourceField())));
    }

    @Test
    void phase13DoesNotTreatQualifiedUserColumnAsSpecialRegisterConstant() {
        SqlSourceFile sourceFile = sqlFile("phase13_user_col.sql", List.of(
                "SELECT T.USER FROM T;"
        ), SqlSourceCategory.TABLE_DIR);
        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult parsed = parser.parse(slice(sourceFile, 1, sourceFile.rawLines().get(0), 0));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(parsed))),
                List.of(parsed)
        );

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_FIELD
                && "T.USER".equals(r.sourceField())));
        assertTrue(rows.stream().noneMatch(r -> r.sourceField().equals("CONSTANT:T.USER")));
    }

    @Test
    void phase11LineAnchoringAndPerLineSequenceUseOriginalRawSqlLines() {
        List<String> rawLines = List.of(
                "INSERT INTO T_DST (A, B)",
                "  SELECT S.X, S.Y",
                "  FROM T_SRC S;",
                "UPDATE T_DST",
                "  SET  A = S.X,  B = S.Y, C = 1",
                "  FROM T_SRC S;"
        );
        SqlSourceFile sourceFile = sqlFile("phase11_anchor.sql", rawLines, SqlSourceCategory.TABLE_DIR);

        SqlStatementParser parser = new SqlStatementParser();
        ParsedStatementResult insert = parser.parse(new StatementSlice(
                sourceFile,
                sourceFile.sourceCategory(),
                "INSERT INTO T_DST (A, B)\nSELECT S.X, S.Y\nFROM T_SRC S;",
                1,
                3,
                List.of(
                        "INSERT INTO T_DST (A, B)",
                        "SELECT S.X, S.Y",
                        "FROM T_SRC S;"
                ),
                0
        ));
        ParsedStatementResult update = parser.parse(new StatementSlice(
                sourceFile,
                sourceFile.sourceCategory(),
                "UPDATE T_DST SET A = S.X, B = S.Y, C = 1 FROM T_SRC S;",
                4,
                6,
                List.of(
                        "UPDATE T_DST",
                        "SET A = S.X, B = S.Y, C = 1",
                        "FROM T_SRC S;"
                ),
                1
        ));

        List<RelationshipRow> rows = new ExtractionPipeline().extract(
                new ExtractionContext(List.of(sourceFile), InMemorySchemaMetadataService.fromParsedStatements(List.of(insert, update))),
                List.of(insert, update)
        );

        RelationshipRow insertMap = rows.stream()
                .filter(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP && "A".equals(r.targetField()) && "S.X".equals(r.sourceField()))
                .findFirst()
                .orElseThrow();
        assertEquals(2, insertMap.lineNo());
        assertEquals("  SELECT S.X, S.Y", insertMap.lineContent());

        List<RelationshipRow> updateSetRows = rows.stream()
                .filter(r -> r.relationship() == RelationshipType.UPDATE_SET_MAP && r.lineNo() == 5)
                .sorted(Comparator.comparingInt(RelationshipRow::lineRelationSeq))
                .toList();
        assertTrue(updateSetRows.size() >= 3);
        assertEquals("  SET  A = S.X,  B = S.Y, C = 1", updateSetRows.get(0).lineContent());

        Map<Integer, List<RelationshipRow>> byLine = rows.stream()
                .collect(Collectors.groupingBy(RelationshipRow::lineNo));
        for (List<RelationshipRow> lineRows : byLine.values()) {
            List<RelationshipRow> ordered = lineRows.stream()
                    .sorted(Comparator.comparingInt(RelationshipRow::lineRelationSeq))
                    .toList();
            for (int i = 0; i < ordered.size(); i++) {
                assertEquals(i, ordered.get(i).lineRelationSeq());
                assertEquals(sourceFile.getRawLine(ordered.get(i).lineNo()), ordered.get(i).lineContent());
            }
        }
    }

    private SqlSourceFile sqlFile(String name, List<String> lines, SqlSourceCategory category) {
        return new SqlSourceFile(
                category,
                Path.of("/tmp/" + name),
                Path.of(name),
                String.join("\n", lines),
                lines
        );
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
