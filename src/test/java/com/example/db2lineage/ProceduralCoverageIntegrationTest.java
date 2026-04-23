package com.example.db2lineage;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ProceduralCoverageIntegrationTest {

    @Test
    void coversDeclareSetDiagnosticsCallAndCursorFetch() throws Exception {
        Path root = Files.createTempDirectory("proc-coverage");
        Path tableDir = Files.createDirectories(root.resolve("table"));
        Path viewDir = Files.createDirectories(root.resolve("view"));
        Path functionDir = Files.createDirectories(root.resolve("function"));
        Path spDir = Files.createDirectories(root.resolve("sp"));
        Path outputDir = Files.createDirectories(root.resolve("out"));

        Path spFile = spDir.resolve("RPT.PR_COVERAGE_TEST.sql");
        Files.writeString(spFile, """
                CREATE PROCEDURE RPT.PR_COVERAGE_TEST
                (
                    IN p_in VARCHAR(10)
                )
                LANGUAGE SQL
                BEGIN
                    DECLARE lv_a INTEGER DEFAULT 1;
                    DECLARE lv_b INTEGER;
                    DECLARE lv_state CHAR(5);
                    DECLARE lv_code INTEGER;
                    DECLARE c_demo CURSOR FOR
                        SELECT SRC.C1, SRC.C2
                          FROM TEMP.SRC SRC;

                    SET lv_code = SQLCODE;
                    SET lv_state = SQLSTATE;
                    SET lv_b = lv_a + 2;
                    SET lv_b = lv_b || '-X';

                    GET DIAGNOSTICS lv_b = ROW_COUNT;

                    CALL TEMP.PR_LOG(lv_a, lv_b, 'INFO');

                    OPEN c_demo;
                    FETCH c_demo INTO lv_a, lv_b;
                    CLOSE c_demo;
                END
                @
                """, StandardCharsets.UTF_8);

        RelationshipDetailMain.main(new String[]{
                "--tableDir", tableDir.toString(),
                "--viewDir", viewDir.toString(),
                "--functionDir", functionDir.toString(),
                "--spDir", spDir.toString(),
                "--outputDir", outputDir.toString()
        });

        List<Map<String, String>> rows = readRows(outputDir.resolve("relationship_detail.tsv"));
        Predicate<Map<String, String>> inProcedure = row ->
                "PROCEDURE".equals(row.get("source_object_type"))
                        && "RPT.PR_COVERAGE_TEST".equals(row.get("source_object"));

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "VARIABLE_SET_MAP".equals(row.get("relationship"))
                        && "CONSTANT:1".equals(row.get("source_field"))
                        && "lv_a".equals(row.get("target_field")))),
                "DECLARE ... DEFAULT should emit VARIABLE_SET_MAP");

        assertTrue(rows.stream().noneMatch(inProcedure.and(row ->
                "VARIABLE_SET_MAP".equals(row.get("relationship"))
                        && "CONSTANT:NULL".equals(row.get("source_field"))
                        && "lv_b".equals(row.get("target_field")))),
                "DECLARE without DEFAULT must not fabricate CONSTANT:NULL VARIABLE_SET_MAP");

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "VARIABLE_SET_MAP".equals(row.get("relationship"))
                        && "lv_a".equals(row.get("source_field"))
                        && "lv_b".equals(row.get("target_field")))),
                "SET arithmetic should include variable participation");

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "VARIABLE_SET_MAP".equals(row.get("relationship"))
                        && "CONSTANT:2".equals(row.get("source_field"))
                        && "lv_b".equals(row.get("target_field")))),
                "SET arithmetic should include literal participation");

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "VARIABLE_SET_MAP".equals(row.get("relationship"))
                        && "CONSTANT:'-X'".equals(row.get("source_field"))
                        && "lv_b".equals(row.get("target_field")))),
                "SET concatenation should include literal participation");

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "DIAGNOSTICS_FETCH_MAP".equals(row.get("relationship"))
                        && "CONSTANT:ROW_COUNT".equals(row.get("source_field"))
                        && "lv_b".equals(row.get("target_field")))),
                "GET DIAGNOSTICS should emit DIAGNOSTICS_FETCH_MAP");

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "DIAGNOSTICS_FETCH_MAP".equals(row.get("relationship"))
                        && "CONSTANT:SQLCODE".equals(row.get("source_field"))
                        && "lv_code".equals(row.get("target_field")))),
                "SET ... = SQLCODE should emit DIAGNOSTICS_FETCH_MAP");

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "DIAGNOSTICS_FETCH_MAP".equals(row.get("relationship"))
                        && "CONSTANT:SQLSTATE".equals(row.get("source_field"))
                        && "lv_state".equals(row.get("target_field")))),
                "SET ... = SQLSTATE should emit DIAGNOSTICS_FETCH_MAP");

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "CALL_PARAM_MAP".equals(row.get("relationship"))
                        && "TEMP.PR_LOG".equals(row.get("target_object"))
                        && "$1".equals(row.get("target_field").trim())
                        && "lv_a".equals(row.get("source_field")))),
                "CALL parameter #1 should be emitted");

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "CALL_PARAM_MAP".equals(row.get("relationship"))
                        && "TEMP.PR_LOG".equals(row.get("target_object"))
                        && "$3".equals(row.get("target_field").trim())
                        && "CONSTANT:'INFO'".equals(row.get("source_field")))),
                "CALL parameter #3 should be emitted");

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "CURSOR_FETCH_MAP".equals(row.get("relationship"))
                        && "C1".equalsIgnoreCase(row.get("source_field"))
                        && "lv_a".equals(row.get("target_field")))),
                "FETCH ... INTO should emit CURSOR_FETCH_MAP for first slot using normalized source column");

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "CURSOR_FETCH_MAP".equals(row.get("relationship"))
                        && "C2".equalsIgnoreCase(row.get("source_field"))
                        && "lv_b".equals(row.get("target_field")))),
                "FETCH ... INTO should emit CURSOR_FETCH_MAP for second slot using normalized source column");

        List<String> callTargetSlots = rows.stream()
                .filter(inProcedure)
                .filter(row -> "CALL_PARAM_MAP".equals(row.get("relationship")))
                .filter(row -> "TEMP.PR_LOG".equals(row.get("target_object")))
                .sorted(java.util.Comparator.comparingInt(row -> Integer.parseInt(row.get("line_no"))))
                .map(row -> row.get("target_field").trim())
                .toList();
        assertTrue(callTargetSlots.containsAll(List.of("$1", "$2", "$3")),
                "CALL params should use positional fallback slots when formal names are not resolvable");

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "CURSOR_DEFINE".equals(row.get("relationship"))
                        && "c_demo".equalsIgnoreCase(row.get("target_object")))),
                "DECLARE cursor should emit CURSOR_DEFINE");

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "CURSOR_READ".equals(row.get("relationship"))
                        && "c_demo".equalsIgnoreCase(row.get("target_object"))
                        && "OPEN".equalsIgnoreCase(row.get("target_field"))
                        && row.get("line_content").trim().startsWith("OPEN"))),
                "OPEN cursor should emit CURSOR_READ");

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "CURSOR_READ".equals(row.get("relationship"))
                        && "c_demo".equalsIgnoreCase(row.get("target_object"))
                        && "CLOSE".equalsIgnoreCase(row.get("target_field"))
                        && row.get("line_content").trim().startsWith("CLOSE"))),
                "CLOSE cursor should emit CURSOR_READ");
    }

    @Test
    void coversPredicateCompletenessForWhereJoinMergeAndControlFlow() throws Exception {
        Path root = Files.createTempDirectory("proc-predicate");
        Path tableDir = Files.createDirectories(root.resolve("table"));
        Path viewDir = Files.createDirectories(root.resolve("view"));
        Path functionDir = Files.createDirectories(root.resolve("function"));
        Path spDir = Files.createDirectories(root.resolve("sp"));
        Path outputDir = Files.createDirectories(root.resolve("out"));

        Path spFile = spDir.resolve("RPT.PR_PREDICATE_TEST.sql");
        Files.writeString(spFile, """
                CREATE PROCEDURE RPT.PR_PREDICATE_TEST
                (
                    IN p_product_type VARCHAR(2),
                    IN p_deal_type    VARCHAR(2)
                )
                LANGUAGE SQL
                BEGIN
                    DECLARE ld_biz_date DATE;
                    DECLARE lv_has_data SMALLINT DEFAULT 0;
                    DECLARE at_end SMALLINT DEFAULT 0;
                    DECLARE lv_month_flag CHAR(1) DEFAULT 'E';
                    DECLARE lv_b INTEGER DEFAULT 0;
                    DECLARE lv_sys_cd VARCHAR(3);

                    DECLARE c_event_detail CURSOR FOR
                        SELECT MAST.EVENT_ID
                          FROM TEMP.TEST_EVENT_MASTER_P MAST
                          INNER JOIN SESSION.TMP_STO_EVENT_SOURCE SRC
                            ON SRC.CUSTOMER_NUMBER = MAST.CUSTOMER_NUMBER
                         WHERE MAST.PRODUCT_TYPE = p_product_type
                           AND MAST.DEAL_TYPE = p_deal_type
                           AND MAST.CREATION_DATE = ld_biz_date
                           AND MAST.EVENT_ID LIKE 'OTC_STO_%'
                           AND MAST.REVERSE_TS IS NULL
                           AND SRC.CLOSE_OUT_AMOUNT IS NOT NULL;

                    SELECT SYS_CD
                      INTO lv_sys_cd
                      FROM TEMP.SYS_WHERE
                     WHERE SYS_CD = 'FOS';

                    SELECT A.C1
                      INTO lv_b
                      FROM TEMP.TA A
                      INNER JOIN TEMP.TB B
                        ON A.C1 = B.C1
                     WHERE A.C2 LIKE 'OTC%'
                       AND A.C3 IS NULL
                       AND A.C4 IS NOT NULL;

                    DELETE FROM TEMP.TA
                     WHERE C2 = p_product_type;

                    UPDATE TEMP.TA T
                       SET C4 = 'Y'
                     WHERE EXISTS (SELECT 1 FROM TEMP.TB B WHERE B.C1 = T.C1);

                    INSERT INTO SESSION.T_OUT (C1)
                    SELECT DEAL.C1
                      FROM TEMP.DEAL DEAL
                      LEFT OUTER JOIN TEMP.POSN POSN
                        ON POSN.CUSTOMER_NUMBER = DEAL.CUSTOMER_NUMBER
                       AND POSN.SUB_ACCOUNT_NUMBER = DEAL.SUB_ACCOUNT_NUMBER
                       AND POSN.DEAL_TYPE = DEAL.DEAL_TYPE
                       AND POSN.DEAL_NUMBER = DEAL.DEAL_NUMBER
                       AND POSN.DEAL_SUB_NUMBER = DEAL.DEAL_SUB_NUMBER
                      LEFT OUTER JOIN TEMP.TMPL TMPL
                        ON TMPL.TEMPLATE_CODE = DEAL.TEMPLATE_CODE
                     WHERE DEAL.PROD_TYPE = p_product_type
                       AND DEAL.DEAL_TYPE = p_deal_type
                       AND DEAL.REVERSE_TS IS NULL
                       AND DEAL.STATUS IN ('A', 'L', 'P')
                       AND TMPL.TEMPLATE_CODE IS NOT NULL
                       AND (DEAL.TRADE_DATE = ld_biz_date
                         OR DEAL.PREMIUM_DATE = ld_biz_date
                         OR DEAL.EXERCISE_DATE = ld_biz_date
                         OR DEAL.EXPIRY_DATE = ld_biz_date
                         OR DEAL.MATURITY_DATE = ld_biz_date);

                    IF lv_has_data = 0 THEN
                        MERGE INTO TEMP.TEST_EVENT_DETAIL_P DETL
                        USING SESSION.TMP_STO_EVENT_SOURCE SRC
                           ON DETL.EVENT_ID = SRC.EVENT_ID
                        WHEN MATCHED AND DETL.REVERSE_TS IS NULL THEN
                            UPDATE SET DETL.SETTLEMENT_AMOUNT = SRC.SETTLEMENT_AMOUNT
                        WHEN NOT MATCHED AND SRC.CLOSE_OUT_AMOUNT IS NOT NULL THEN
                            INSERT (EVENT_ID, SETTLEMENT_AMOUNT)
                            VALUES (SRC.EVENT_ID, SRC.SETTLEMENT_AMOUNT);
                    END IF;

                    IF at_end = 1 THEN
                        SET lv_has_data = 0;
                    END IF;

                    SET ld_biz_date = CASE
                        WHEN lv_month_flag = 'E' THEN ld_biz_date
                        ELSE ld_biz_date
                    END;
                END
                @
                """, StandardCharsets.UTF_8);

        RelationshipDetailMain.main(new String[]{
                "--tableDir", tableDir.toString(),
                "--viewDir", viewDir.toString(),
                "--functionDir", functionDir.toString(),
                "--spDir", spDir.toString(),
                "--outputDir", outputDir.toString()
        });

        List<Map<String, String>> rows = readRows(outputDir.resolve("relationship_detail.tsv"));
        Predicate<Map<String, String>> inProcedure = row ->
                "PROCEDURE".equals(row.get("source_object_type"))
                        && "RPT.PR_PREDICATE_TEST".equals(row.get("source_object"));

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "WHERE".equals(row.get("relationship"))
                        && row.get("source_field").toUpperCase().endsWith("C2"))),
                "WHERE should include field side token");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "WHERE".equals(row.get("relationship"))
                        && row.get("source_field").toUpperCase().endsWith("SYS_CD"))),
                "SELECT ... INTO ... WHERE SYS_CD = 'FOS' should emit WHERE field token");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "WHERE".equals(row.get("relationship"))
                        && "SYS_CD".equalsIgnoreCase(row.get("source_field"))
                        && "TABLE".equals(row.get("target_object_type"))
                        && "TEMP.SYS_WHERE".equalsIgnoreCase(row.get("target_object"))
                        && "SYS_CD".equalsIgnoreCase(row.get("target_field")))),
                "SYS_CD predicate should resolve to TEMP.SYS_WHERE.SYS_CD target side");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "WHERE".equals(row.get("relationship"))
                        && "CONSTANT:'FOS'".equals(row.get("source_field")))),
                "SELECT ... INTO ... WHERE SYS_CD = 'FOS' should emit WHERE literal token");
        assertTrue(rows.stream().noneMatch(inProcedure.and(row ->
                "WHERE".equals(row.get("relationship"))
                        && "SYS_CD".equalsIgnoreCase(row.get("source_field"))
                        && "UNKNOWN".equals(row.get("target_object_type")))),
                "Do not keep UNKNOWN-target SYS_CD predicate row when resolved owner exists");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "WHERE".equals(row.get("relationship"))
                        && "CONSTANT:'OTC%'".equals(row.get("source_field")))),
                "LIKE predicates should include literal-side token");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "WHERE".equals(row.get("relationship"))
                        && row.get("source_field").toUpperCase().endsWith("C3"))),
                "IS NULL should preserve nullable field token in WHERE");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "WHERE".equals(row.get("relationship"))
                        && "CONSTANT:NULL".equals(row.get("source_field")))),
                "IS NULL should include CONSTANT:NULL in WHERE");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "WHERE".equals(row.get("relationship"))
                        && row.get("source_field").toUpperCase().endsWith("C4"))),
                "IS NOT NULL should preserve nullable field token in WHERE");

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "JOIN_ON".equals(row.get("relationship"))
                        && row.get("source_field").toUpperCase().endsWith("C1"))),
                "JOIN_ON should include direct join tokens");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "JOIN_ON".equals(row.get("relationship"))
                        && row.get("source_field").toUpperCase().endsWith("B.C1"))),
                "JOIN_ON should include the joined-table side token");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "JOIN_ON".equals(row.get("relationship"))
                        && row.get("source_field").toUpperCase().endsWith("CUSTOMER_NUMBER"))),
                "INSERT...SELECT LEFT JOIN should include CUSTOMER_NUMBER join token");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "JOIN_ON".equals(row.get("relationship"))
                        && row.get("source_field").toUpperCase().endsWith("TEMPLATE_CODE"))),
                "INSERT...SELECT LEFT JOIN should include TEMPLATE_CODE join token");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "WHERE".equals(row.get("relationship"))
                        && "p_product_type".equalsIgnoreCase(row.get("source_field")))),
                "DELETE predicate should include variable-side WHERE token");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "WHERE".equals(row.get("relationship"))
                        && "CONSTANT:'A'".equals(row.get("source_field")))),
                "IN-list predicate should include first literal");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "WHERE".equals(row.get("relationship"))
                        && "CONSTANT:'L'".equals(row.get("source_field")))),
                "IN-list predicate should include second literal");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "WHERE".equals(row.get("relationship"))
                        && "CONSTANT:'P'".equals(row.get("source_field")))),
                "IN-list predicate should include third literal");
        long bizDateRows = rows.stream()
                .filter(inProcedure)
                .filter(row -> "WHERE".equals(row.get("relationship")))
                .filter(row -> "ld_biz_date".equalsIgnoreCase(row.get("source_field")))
                .count();
        assertTrue(bizDateRows >= 1, "OR-connected date predicates should emit ld_biz_date participation without over-constraining branch-level duplication");

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "MERGE_MATCH".equals(row.get("relationship"))
                        && row.get("source_field").toUpperCase().endsWith("EVENT_ID"))),
                "MERGE_MATCH should include ON predicate direct tokens");

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "CONTROL_FLOW_CONDITION".equals(row.get("relationship"))
                        && "lv_has_data".equals(row.get("source_field")))),
                "IF condition should include variable-side token");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "CONTROL_FLOW_CONDITION".equals(row.get("relationship"))
                        && "lv_has_data".equals(row.get("source_field"))
                        && "lv_has_data".equals(row.get("target_field")))),
                "IF condition target_field should be populated for lv_has_data");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "CONTROL_FLOW_CONDITION".equals(row.get("relationship"))
                        && "at_end".equals(row.get("source_field")))),
                "Second IF condition should include variable-side token");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "CONTROL_FLOW_CONDITION".equals(row.get("relationship"))
                        && "at_end".equals(row.get("source_field"))
                        && "at_end".equals(row.get("target_field")))),
                "IF condition target_field should be populated for at_end");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "SELECT_EXPR".equals(row.get("relationship"))
                        && "lv_month_flag".equals(row.get("source_field")))),
                "Value CASE WHEN condition should emit SELECT_EXPR token");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                "SELECT_EXPR".equals(row.get("relationship"))
                        && "CONSTANT:'E'".equals(row.get("source_field"))
                        && "UNKNOWN".equals(row.get("target_object_type"))
                        && "UNKNOWN_SELECT_EXPR".equals(row.get("target_object")))),
                "Value CASE WHEN literal should emit UNKNOWN/UNKNOWN_SELECT_EXPR SELECT_EXPR token");
        assertTrue(rows.stream().noneMatch(inProcedure.and(row ->
                "CONTROL_FLOW_CONDITION".equals(row.get("relationship"))
                        && "lv_month_flag".equals(row.get("source_field")))),
                "Value CASE WHEN should not be emitted as CONTROL_FLOW_CONDITION");
        assertTrue(rows.stream().noneMatch(inProcedure.and(row ->
                "CONTROL_FLOW_CONDITION".equals(row.get("relationship"))
                        && "CLOSE_OUT_AMOUNT".equals(row.get("source_field")))),
                "Control-flow extraction must not fabricate predicate tokens from unrelated SQL clauses");

        long focusedRows = rows.stream()
                .filter(inProcedure)
                .filter(row -> List.of("WHERE", "JOIN_ON", "MERGE_MATCH", "CONTROL_FLOW_CONDITION").contains(row.get("relationship")))
                .count();
        long focusedDistinct = rows.stream()
                .filter(inProcedure)
                .filter(row -> List.of("WHERE", "JOIN_ON", "MERGE_MATCH", "CONTROL_FLOW_CONDITION").contains(row.get("relationship")))
                .map(row -> row.get("relationship") + "|" + row.get("line_no") + "|" + row.get("source_field"))
                .distinct()
                .count();
        assertTrue(focusedDistinct >= (focusedRows * 3) / 5,
                "Predicate/control-flow rows should avoid broad duplication across overlapping extraction paths");
    }

    @Test
    void coversProceduralInsertSelectAndValuesWithoutInsertShapeCrash() throws Exception {
        Path root = Files.createTempDirectory("proc-insert-shape");
        Path tableDir = Files.createDirectories(root.resolve("table"));
        Path viewDir = Files.createDirectories(root.resolve("view"));
        Path functionDir = Files.createDirectories(root.resolve("function"));
        Path spDir = Files.createDirectories(root.resolve("sp"));
        Path outputDir = Files.createDirectories(root.resolve("out"));

        Path spFile = spDir.resolve("RPT.PR_INSERT_SHAPE_TEST.sql");
        Files.writeString(spFile, """
                CREATE PROCEDURE RPT.PR_INSERT_SHAPE_TEST()
                LANGUAGE SQL
                BEGIN
                    DECLARE v_a INTEGER DEFAULT 1;
                    DECLARE v_b INTEGER DEFAULT 2;

                    DECLARE GLOBAL TEMPORARY TABLE SESSION.T_TMP
                    (
                        C1 INTEGER,
                        C2 INTEGER
                    )
                    WITH REPLACE
                    ON COMMIT PRESERVE ROWS
                    NOT LOGGED;

                    INSERT INTO SESSION.T_TMP (C1, C2)
                    SELECT v_a, v_b
                      FROM SYSIBM.SYSDUMMY1;

                    INSERT INTO SESSION.T_TMP (C1, C2)
                    VALUES (v_b, 99);
                END
                @
                """, StandardCharsets.UTF_8);

        RelationshipDetailMain.main(new String[]{
                "--tableDir", tableDir.toString(),
                "--viewDir", viewDir.toString(),
                "--functionDir", functionDir.toString(),
                "--spDir", spDir.toString(),
                "--outputDir", outputDir.toString()
        });

        List<Map<String, String>> rows = readRows(outputDir.resolve("relationship_detail.tsv"));
        Predicate<Map<String, String>> inProcedure = row ->
                "PROCEDURE".equals(row.get("source_object_type"))
                        && "RPT.PR_INSERT_SHAPE_TEST".equals(row.get("source_object"));

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                        "INSERT_SELECT_MAP".equals(row.get("relationship"))
                                && "C1".equalsIgnoreCase(row.get("target_field"))
                                && "v_a".equalsIgnoreCase(row.get("source_field")))),
                "INSERT ... SELECT should still emit INSERT_SELECT_MAP for slot 1");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                        "INSERT_SELECT_MAP".equals(row.get("relationship"))
                                && "C2".equalsIgnoreCase(row.get("target_field"))
                                && "v_b".equalsIgnoreCase(row.get("source_field")))),
                "INSERT ... SELECT should still emit INSERT_SELECT_MAP for slot 2");

        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                        "INSERT_SELECT_MAP".equals(row.get("relationship"))
                                && "C1".equalsIgnoreCase(row.get("target_field"))
                                && "v_b".equalsIgnoreCase(row.get("source_field")))),
                "INSERT ... VALUES should emit INSERT_SELECT_MAP for variable value");
        assertTrue(rows.stream().anyMatch(inProcedure.and(row ->
                        "INSERT_SELECT_MAP".equals(row.get("relationship"))
                                && "C2".equalsIgnoreCase(row.get("target_field"))
                                && "CONSTANT:99".equals(row.get("source_field")))),
                "INSERT ... VALUES should emit INSERT_SELECT_MAP for literal value");
    }

    private static List<Map<String, String>> readRows(Path tsvPath) throws IOException {
        List<String> lines = Files.readAllLines(tsvPath, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            return List.of();
        }
        String[] header = lines.get(0).split("\t", -1);
        List<Map<String, String>> rows = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {
            String[] values = lines.get(i).split("\t", -1);
            int max = Math.min(header.length, values.length);
            rows.add(java.util.stream.IntStream.range(0, max)
                    .boxed()
                    .collect(Collectors.toMap(idx -> header[idx], idx -> values[idx])));
        }
        return rows;
    }
}
