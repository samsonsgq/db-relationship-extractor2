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
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;

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

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.DIAGNOSTICS_FETCH_MAP
                && r.lineNo() == 98
                && "CONSTANT:SQLCODE".equals(r.sourceField())
                && "ln_sqlcode".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.DIAGNOSTICS_FETCH_MAP
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
                && "$1".equalsIgnoreCase(r.targetField().trim())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP
                && r.lineNo() == 170
                && "$4".equalsIgnoreCase(r.targetField().trim())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP
                && r.lineNo() == 117
                && "$1 ".equals(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP
                && r.lineNo() == 167
                && "$1 ".equals(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP
                && r.lineNo() == 208
                && "$1 ".equals(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP
                && r.lineNo() == 241
                && "$1 ".equals(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP
                && r.lineNo() == 919
                && "$1 ".equals(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP
                && r.lineNo() == 926
                && "$1 ".equals(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_PROCEDURE
                && r.lineNo() == 371
                && "TEMP.PR_PROCEDURE_LOG".equalsIgnoreCase(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP
                && r.lineNo() == 372
                && "lv_msg_category".equalsIgnoreCase(r.sourceField())
                && "$1".equalsIgnoreCase(r.targetField().trim())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP
                && r.lineNo() == 372
                && "$1".equals(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP
                && r.lineNo() == 375
                && "CONSTANT:'No source rows found for structured option events'".equalsIgnoreCase(r.sourceField())
                && "$4".equalsIgnoreCase(r.targetField().trim())));
        assertEquals(1, rows.stream()
                .filter(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP
                        && r.lineNo() == 372
                        && "lv_msg_category".equalsIgnoreCase(r.sourceField())
                        && "$1".equalsIgnoreCase(r.targetField().trim()))
                .count());

        assertHasVariableSet(rows, 201, "lv_batch_comment", "CONSTANT:'biz_date='");
        assertHasVariableSet(rows, 201, "lv_batch_comment", "ld_biz_date");
        assertHasVariableSet(rows, 202, "lv_batch_comment", "CONSTANT:', prev_biz_date='");
        assertHasVariableSet(rows, 202, "lv_batch_comment", "ld_last_biz_date");
        assertHasVariableSet(rows, 203, "lv_batch_comment", "CONSTANT:', next_biz_date='");
        assertHasVariableSet(rows, 203, "lv_batch_comment", "ld_next_biz_date");
        assertHasVariableSet(rows, 204, "lv_batch_comment", "CONSTANT:', region='");
        assertHasVariableSet(rows, 204, "lv_batch_comment", "lv_region_code");
        assertHasVariableSet(rows, 204, "lv_batch_comment", "CONSTANT:''");
        assertHasVariableSet(rows, 205, "lv_batch_comment", "CONSTANT:', month_flag='");
        assertHasVariableSet(rows, 205, "lv_batch_comment", "lv_month_flag");
        assertHasVariableSet(rows, 205, "lv_batch_comment", "CONSTANT:''");

        assertHasVariableSet(rows, 914, "lv_batch_comment", "CONSTANT:'Completed. inserted='");
        assertHasVariableSet(rows, 914, "lv_batch_comment", "lv_insert_count");
        assertHasVariableSet(rows, 915, "lv_batch_comment", "CONSTANT:', updated='");
        assertHasVariableSet(rows, 915, "lv_batch_comment", "lv_update_count");
        assertHasVariableSet(rows, 916, "lv_batch_comment", "CONSTANT:', merged='");
        assertHasVariableSet(rows, 916, "lv_batch_comment", "lv_merge_count");
        assertHasVariableSet(rows, 107, "lv_error_message", "CONSTANT:' failed.'");
        assertHasVariableSet(rows, 107, "lv_error_message", "lv_procedure_name");
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                && r.lineNo() == 107
                && r.lineRelationSeq() == 2
                && "lv_error_message".equalsIgnoreCase(r.targetField())
                && "CONSTANT:' failed.'".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                && r.lineNo() == 107
                && r.lineRelationSeq() == 3
                && "lv_error_message".equalsIgnoreCase(r.targetField())
                && "lv_procedure_name".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                && r.lineNo() == 108
                && r.lineRelationSeq() == 4
                && "lv_error_message".equalsIgnoreCase(r.targetField())
                && "CONSTANT:''".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                && r.lineNo() == 108
                && r.lineRelationSeq() == 5
                && "lv_error_message".equalsIgnoreCase(r.targetField())
                && "p_product_type".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                && r.lineNo() == 196
                && "ld_end_date".equalsIgnoreCase(r.targetField())
                && "ld_actual_month_end_date".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                && r.lineNo() == 204
                && r.lineRelationSeq() == 3
                && "lv_batch_comment".equalsIgnoreCase(r.targetField())
                && "CONSTANT:''".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                && r.lineNo() == 204
                && r.lineRelationSeq() == 4
                && "lv_batch_comment".equalsIgnoreCase(r.targetField())
                && "CONSTANT:', region='".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                && r.lineNo() == 204
                && r.lineRelationSeq() == 5
                && "lv_batch_comment".equalsIgnoreCase(r.targetField())
                && "lv_region_code".equalsIgnoreCase(r.sourceField())));

        assertHasVariableSet(rows, 14, "lv_msg_category", "CONSTANT:'ARG'");
        assertHasVariableSet(rows, 19, "lv_sqlstate", "CONSTANT:'00000'");
        assertHasVariableSet(rows, 22, "lv_source_system", "CONSTANT:'STO'");
        assertHasVariableSet(rows, 23, "lv_process_status", "CONSTANT:'START'");
        assertHasVariableSet(rows, 25, "lv_row_count", "CONSTANT:0");
        assertHasVariableSet(rows, 26, "lv_insert_count", "CONSTANT:0");
        assertHasVariableSet(rows, 27, "lv_update_count", "CONSTANT:0");
        assertHasVariableSet(rows, 28, "lv_merge_count", "CONSTANT:0");
        assertHasVariableSet(rows, 29, "lv_has_data", "CONSTANT:0");
        assertHasVariableSet(rows, 38, "ln_sqlcode", "CONSTANT:0");
        assertHasVariableSet(rows, 59, "at_end", "CONSTANT:0");
        assertHasVariableSet(rows, 130, "at_end", "CONSTANT:1");

        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                && r.lineNo() == 16
                && "lv_err_pos".equalsIgnoreCase(r.targetField())));

        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                && r.lineNo() == 99
                && "lv_sqlstate".equalsIgnoreCase(r.targetField())));

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 107
                && "lv_procedure_name".equalsIgnoreCase(r.sourceField())
                && "VARIABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "RPT.PR_TEST_DEMO".equalsIgnoreCase(r.targetObject())
                && "lv_procedure_name".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 205
                && "lv_month_flag".equalsIgnoreCase(r.sourceField())
                && "VARIABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "RPT.PR_TEST_DEMO".equalsIgnoreCase(r.targetObject())
                && "lv_month_flag".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 284
                && "TRADE_DATE".equalsIgnoreCase(r.sourceField())
                && "TABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "TEMP.STRUCTURED_OPTION_DEAL_BEF_EOD".equalsIgnoreCase(r.targetObject())
                && "TRADE_DATE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 299
                && "TRADE_DATE".equalsIgnoreCase(r.sourceField())
                && "TEMP.STRUCTURED_OPTION_DEAL_BEF_EOD".equalsIgnoreCase(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 299
                && "ld_biz_date".equalsIgnoreCase(r.sourceField())
                && "VARIABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "RPT.PR_TEST_DEMO".equalsIgnoreCase(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 299
                && "BUY_SELL_FLAG".equalsIgnoreCase(r.sourceField())
                && "TEMP.STRUCTURED_OPTION_DEAL_BEF_EOD".equalsIgnoreCase(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 299
                && "CONSTANT:'B'".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 287
                && "CONSTANT:'N'".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 292
                && "TRADE_DATE".equalsIgnoreCase(r.sourceField())
                && "TEMP.STRUCTURED_OPTION_DEAL_BEF_EOD".equalsIgnoreCase(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 292
                && "ld_biz_date".equalsIgnoreCase(r.sourceField())
                && "VARIABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "RPT.PR_TEST_DEMO".equalsIgnoreCase(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 326
                && "CONSTANT:'N'".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 326
                && "KNOCK_IN_FLAG".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 327
                && "KNOCK_OUT_FLAG".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 402
                && "ld_biz_date".equalsIgnoreCase(r.sourceField())
                && "VARIABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "RPT.PR_TEST_DEMO".equalsIgnoreCase(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 403
                && "ld_biz_date".equalsIgnoreCase(r.sourceField())
                && "VARIABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "RPT.PR_TEST_DEMO".equalsIgnoreCase(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 407
                && "SEQUENCE:RPT.GLOBAL_SEQ_ACCOUNT_EVENT_UNIQUE_ID".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 189
                && "SYS_CD".equalsIgnoreCase(r.sourceField())
                && "TABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "TEMP.SYS_WHERE".equalsIgnoreCase(r.targetObject())
                && "SYS_CD".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 189
                && "CONSTANT:'FOS'".equalsIgnoreCase(r.sourceField())
                && "TABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "TEMP.SYS_WHERE".equalsIgnoreCase(r.targetObject())
                && "SYS_CD".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 341
                && "PROD_TYPE".equalsIgnoreCase(r.sourceField())
                && "TABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "TEMP.STRUCTURED_OPTION_DEAL_BEF_EOD".equalsIgnoreCase(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 343
                && "CONSTANT:NULL".equalsIgnoreCase(r.sourceField())
                && "TABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "TEMP.STRUCTURED_OPTION_DEAL_BEF_EOD".equalsIgnoreCase(r.targetObject())
                && "REVERSE_TS".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 344
                && "CONSTANT:'A'".equalsIgnoreCase(r.sourceField())
                && "STATUS".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 345
                && "CONSTANT:NULL".equalsIgnoreCase(r.sourceField())
                && "TEMP.STRUCTURED_OPTION_TEMPLATE".equalsIgnoreCase(r.targetObject())
                && "TEMPLATE_CODE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 348
                && "TRADE_DATE".equalsIgnoreCase(r.sourceField())
                && "TABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "TEMP.STRUCTURED_OPTION_DEAL_BEF_EOD".equalsIgnoreCase(r.targetObject())
                && "TRADE_DATE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 348
                && "ld_biz_date".equalsIgnoreCase(r.sourceField())
                && "VARIABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "RPT.PR_TEST_DEMO".equalsIgnoreCase(r.targetObject())
                && "ld_biz_date".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 349
                && "PREMIUM_DATE".equalsIgnoreCase(r.sourceField())
                && "TEMP.STRUCTURED_OPTION_DEAL_BEF_EOD".equalsIgnoreCase(r.targetObject())
                && "PREMIUM_DATE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 350
                && "EXERCISE_DATE".equalsIgnoreCase(r.sourceField())
                && "TEMP.STRUCTURED_OPTION_DEAL_BEF_EOD".equalsIgnoreCase(r.targetObject())
                && "EXERCISE_DATE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 351
                && "EXPIRY_DATE".equalsIgnoreCase(r.sourceField())
                && "TEMP.STRUCTURED_OPTION_DEAL_BEF_EOD".equalsIgnoreCase(r.targetObject())
                && "EXPIRY_DATE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 352
                && "MATURITY_DATE".equalsIgnoreCase(r.sourceField())
                && "TEMP.STRUCTURED_OPTION_DEAL_BEF_EOD".equalsIgnoreCase(r.targetObject())
                && "MATURITY_DATE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 762
                && "CUST_NUM".equalsIgnoreCase(r.sourceField())
                && "TEMP.STRUCTURED_OPTION_SOD".equalsIgnoreCase(r.targetObject())
                && "CUST_NUM".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 762
                && "CUSTOMER_NUMBER".equalsIgnoreCase(r.sourceField())
                && "TEMP.TEST_EVENT_MASTER_P".equalsIgnoreCase(r.targetObject())
                && "CUSTOMER_NUMBER".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 763
                && "SB_ACCT_NUM".equalsIgnoreCase(r.sourceField())
                && "TEMP.STRUCTURED_OPTION_SOD".equalsIgnoreCase(r.targetObject())
                && "SB_ACCT_NUM".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 764
                && "DEAL_NUM".equalsIgnoreCase(r.sourceField())
                && "TEMP.STRUCTURED_OPTION_SOD".equalsIgnoreCase(r.targetObject())
                && "DEAL_NUM".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 765
                && "DEAL_SB_NUM".equalsIgnoreCase(r.sourceField())
                && "TEMP.STRUCTURED_OPTION_SOD".equalsIgnoreCase(r.targetObject())
                && "DEAL_SB_NUM".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 762
                && "CUST_NUM".equalsIgnoreCase(r.sourceField())
                && "TEMP.TEST_EVENT_MASTER_P".equalsIgnoreCase(r.targetObject())
                && "CUST_NUM".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 763
                && "SB_ACCT_NUM".equalsIgnoreCase(r.sourceField())
                && "TEMP.TEST_EVENT_MASTER_P".equalsIgnoreCase(r.targetObject())
                && "SB_ACCT_NUM".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 764
                && "DEAL_NUM".equalsIgnoreCase(r.sourceField())
                && "TEMP.TEST_EVENT_MASTER_P".equalsIgnoreCase(r.targetObject())
                && "DEAL_NUM".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 765
                && "DEAL_SB_NUM".equalsIgnoreCase(r.sourceField())
                && "TEMP.TEST_EVENT_MASTER_P".equalsIgnoreCase(r.targetObject())
                && "DEAL_SB_NUM".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 766
                && "CONSTANT:1".equalsIgnoreCase(r.sourceField())
                && "UNKNOWN_SELECT_EXPR".equalsIgnoreCase(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 776
                && "CUST_NUM".equalsIgnoreCase(r.sourceField())
                && "TEMP.STRUCTURED_OPTION_SOD".equalsIgnoreCase(r.targetObject())
                && "CUST_NUM".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 776
                && "CUSTOMER_NUMBER".equalsIgnoreCase(r.sourceField())
                && "TEMP.TEST_EVENT_MASTER_P".equalsIgnoreCase(r.targetObject())
                && "CUSTOMER_NUMBER".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 777
                && "SB_ACCT_NUM".equalsIgnoreCase(r.sourceField())
                && "TEMP.STRUCTURED_OPTION_SOD".equalsIgnoreCase(r.targetObject())
                && "SB_ACCT_NUM".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 778
                && "DEAL_NUM".equalsIgnoreCase(r.sourceField())
                && "TEMP.STRUCTURED_OPTION_SOD".equalsIgnoreCase(r.targetObject())
                && "DEAL_NUM".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 779
                && "DEAL_SB_NUM".equalsIgnoreCase(r.sourceField())
                && "TEMP.STRUCTURED_OPTION_SOD".equalsIgnoreCase(r.targetObject())
                && "DEAL_SB_NUM".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.UPDATE_TABLE
                && r.lineNo() == 756
                && "TEMP.TEST_EVENT_MASTER_P".equalsIgnoreCase(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.UPDATE_TARGET_COL
                && r.lineNo() == 757
                && "TEMP.TEST_EVENT_MASTER_P".equalsIgnoreCase(r.targetObject())
                && "DEAL_NUMBER".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.UPDATE_TARGET_COL
                && r.lineNo() == 757
                && "TEMP.TEST_EVENT_MASTER_P".equalsIgnoreCase(r.targetObject())
                && "DEAL_SUB_NUMBER".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_FIELD
                && r.lineNo() == 759
                && "ORIG_DEAL_NUM".equalsIgnoreCase(r.sourceField())
                && "TEMP.STRUCTURED_OPTION_SOD".equalsIgnoreCase(r.targetObject())
                && "ORIG_DEAL_NUM".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_FIELD
                && r.lineNo() == 760
                && "ORIG_DEAL_SB_NUM".equalsIgnoreCase(r.sourceField())
                && "TEMP.STRUCTURED_OPTION_SOD".equalsIgnoreCase(r.targetObject())
                && "ORIG_DEAL_SB_NUM".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.UPDATE_SET_MAP
                && r.lineNo() == 759
                && "ORIG_DEAL_NUM".equalsIgnoreCase(r.sourceField())
                && "TEMP.TEST_EVENT_MASTER_P".equalsIgnoreCase(r.targetObject())
                && "DEAL_NUMBER".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.UPDATE_SET_MAP
                && r.lineNo() == 760
                && "ORIG_DEAL_SB_NUM".equalsIgnoreCase(r.sourceField())
                && "TEMP.TEST_EVENT_MASTER_P".equalsIgnoreCase(r.targetObject())
                && "DEAL_SUB_NUMBER".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_TABLE
                && r.lineNo() == 761
                && "TEMP.STRUCTURED_OPTION_SOD".equalsIgnoreCase(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_TABLE
                && r.lineNo() == 775
                && "TEMP.STRUCTURED_OPTION_SOD".equalsIgnoreCase(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 800
                && "CREATION_DATE".equalsIgnoreCase(r.sourceField())
                && "TEMP.TEST_EVENT_MASTER_P".equalsIgnoreCase(r.targetObject())
                && "CREATION_DATE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 800
                && "ld_biz_date".equalsIgnoreCase(r.sourceField())
                && "VARIABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "RPT.PR_TEST_DEMO".equalsIgnoreCase(r.targetObject())
                && "ld_biz_date".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 801
                && "PRODUCT_TYPE".equalsIgnoreCase(r.sourceField())
                && "TEMP.TEST_EVENT_MASTER_P".equalsIgnoreCase(r.targetObject())
                && "PRODUCT_TYPE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 801
                && "p_product_type".equalsIgnoreCase(r.sourceField())
                && "VARIABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "RPT.PR_TEST_DEMO".equalsIgnoreCase(r.targetObject())
                && "p_product_type".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 802
                && "DEAL_TYPE".equalsIgnoreCase(r.sourceField())
                && "TEMP.TEST_EVENT_MASTER_P".equalsIgnoreCase(r.targetObject())
                && "DEAL_TYPE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 802
                && "p_deal_type".equalsIgnoreCase(r.sourceField())
                && "VARIABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "RPT.PR_TEST_DEMO".equalsIgnoreCase(r.targetObject())
                && "p_deal_type".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 803
                && "EVENT_ID".equalsIgnoreCase(r.sourceField())
                && "TEMP.TEST_EVENT_MASTER_P".equalsIgnoreCase(r.targetObject())
                && "EVENT_ID".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 803
                && "CONSTANT:'OTC_STO_%'".equalsIgnoreCase(r.sourceField())
                && "TEMP.TEST_EVENT_MASTER_P".equalsIgnoreCase(r.targetObject())
                && "EVENT_ID".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 451
                && "CONSTANT:'OTC_STO_%'".equalsIgnoreCase(r.sourceField())
                && "TABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "TEMP.TEST_EVENT_MASTER_P".equalsIgnoreCase(r.targetObject())
                && "EVENT_ID".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 348
                && "UNKNOWN".equalsIgnoreCase(r.targetObjectType().name())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 762
                && "UNKNOWN".equalsIgnoreCase(r.targetObjectType().name())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 776
                && "UNKNOWN".equalsIgnoreCase(r.targetObjectType().name())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.WHERE
                && r.lineNo() == 800
                && "UNKNOWN".equalsIgnoreCase(r.targetObjectType().name())));

        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_FIELD
                && r.lineNo() == 178
                && "BIZ_DT".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_FIELD
                && r.lineNo() == 179
                && "PREV_BIZ_DT".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                && r.lineNo() == 178
                && "BIZ_DT".equalsIgnoreCase(r.sourceField())
                && "ld_biz_date".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                && r.lineNo() == 179
                && "PREV_BIZ_DT".equalsIgnoreCase(r.sourceField())
                && "ld_last_biz_date".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                && r.lineNo() == 180
                && "NEXT_BIZ_DT".equalsIgnoreCase(r.sourceField())
                && "ld_next_biz_date".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                && r.lineNo() == 181
                && "MONTH_FLAG".equalsIgnoreCase(r.sourceField())
                && "lv_month_flag".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                && r.lineNo() == 182
                && "REGION_CD".equalsIgnoreCase(r.sourceField())
                && "lv_region_code".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_FIELD
                && r.lineNo() == 277
                && "CUSTOMER_NUMBER".equalsIgnoreCase(r.sourceField())
                && "TABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "TEMP.STRUCTURED_OPTION_DEAL_BEF_EOD".equalsIgnoreCase(r.targetObject())
                && "CUSTOMER_NUMBER".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.SELECT_FIELD
                && r.lineNo() == 284
                && "CONSTANT:'TRADE'".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_TARGET_COL
                && r.lineNo() == 254
                && "SESSION_TABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "SESSION.TMP_STO_EVENT_SOURCE".equalsIgnoreCase(r.targetObject())
                && "CUSTOMER_NUMBER".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_TARGET_COL
                && r.lineNo() == 258
                && "SESSION_TABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "SESSION.TMP_STO_EVENT_SOURCE".equalsIgnoreCase(r.targetObject())
                && "PRODUCT_TYPE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_TARGET_COL
                && r.lineNo() == 399
                && "TABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "TEMP.TEST_EVENT_MASTER_P".equalsIgnoreCase(r.targetObject())
                && "DEAL_SUB_NUMBER".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_TARGET_COL
                && r.lineNo() == 525
                && "TABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "TEMP.TEST_EVENT_DETAIL_P".equalsIgnoreCase(r.targetObject())
                && "DEAL_TYPE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_TARGET_COL
                && r.lineNo() == 729
                && "TABLE".equalsIgnoreCase(r.targetObjectType().name())
                && "TEMP.TEST_EVENT_DETAIL_P".equalsIgnoreCase(r.targetObject())
                && "FIELD_VALUE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP
                && r.lineNo() == 277
                && "CUSTOMER_NUMBER".equalsIgnoreCase(r.sourceField())
                && "CUSTOMER_NUMBER".equalsIgnoreCase(r.targetField())
                && "SESSION_TABLE".equalsIgnoreCase(r.targetObjectType().name())
                && r.lineRelationSeq() == 1));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP
                && r.lineNo() == 289
                && "CONSTANT:'OTHER'".equalsIgnoreCase(r.sourceField())
                && "EVENT_CLASS".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP
                && r.lineNo() == 292
                && "TRADE_DATE".equalsIgnoreCase(r.sourceField())
                && "EVENT_DATE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP
                && r.lineNo() == 293
                && "PREMIUM_DATE".equalsIgnoreCase(r.sourceField())
                && "EVENT_DATE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP
                && r.lineNo() == 294
                && "EXERCISE_DATE".equalsIgnoreCase(r.sourceField())
                && "EVENT_DATE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP
                && r.lineNo() == 295
                && "EXPIRY_DATE".equalsIgnoreCase(r.sourceField())
                && "EVENT_DATE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP
                && r.lineNo() == 401
                && "CONSTANT:'A'".equalsIgnoreCase(r.sourceField())
                && "ACTIVE_FLAG".equalsIgnoreCase(r.targetField())
                && "TEMP.TEST_EVENT_MASTER_P".equalsIgnoreCase(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP
                && r.lineNo() == 403
                && "ld_biz_date".equalsIgnoreCase(r.sourceField())
                && "AS_OF_DATE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SEQUENCE_VALUE_MAP
                && r.lineNo() == 407
                && "SEQUENCE:RPT.GLOBAL_SEQ_ACCOUNT_EVENT_UNIQUE_ID".equalsIgnoreCase(r.sourceField())
                && "SEQUENCE_NUMBER".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP
                && r.lineNo() == 407
                && "SEQUENCE:RPT.GLOBAL_SEQ_ACCOUNT_EVENT_UNIQUE_ID".equalsIgnoreCase(r.sourceField())
                && "SEQUENCE_NUMBER".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP
                && r.lineNo() == 533
                && "CONSTANT:'A'".equalsIgnoreCase(r.sourceField())
                && "ACTIVE_FLAG".equalsIgnoreCase(r.targetField())
                && "TEMP.TEST_EVENT_DETAIL_P".equalsIgnoreCase(r.targetObject())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP
                && r.lineNo() == 541
                && "cv_premium_amount".equalsIgnoreCase(r.sourceField())
                && "FIELD_VALUE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP
                && r.lineNo() == 541
                && "CONSTANT:'0'".equalsIgnoreCase(r.sourceField())
                && "FIELD_VALUE".equalsIgnoreCase(r.targetField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 541
                && "cv_premium_amount".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 541
                && "CONSTANT:'0'".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 541
                && "FUNCTION:COALESCE".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 541
                && "FUNCTION:CHAR".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 716
                && "cv_expiry_date".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 716
                && "CONSTANT:''".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().noneMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 716
                && "FUNCTION:CHAR".equalsIgnoreCase(r.sourceField())));
        assertTrue(rows.stream().anyMatch(r -> r.relationship() == RelationshipType.SELECT_EXPR
                && r.lineNo() == 741
                && "cv_exercise_date".equalsIgnoreCase(r.sourceField())));
        assertSelectExprRowsBeforeInsertSelectMapRows(rows, 541);
        assertSelectExprRowsBeforeInsertSelectMapRows(rows, 716);
        assertSelectExprRowsBeforeInsertSelectMapRows(rows, 741);

        List<String> dupKeys = rows.stream()
                .filter(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP)
                .map(r -> r.lineNo() + "|" + r.sourceField() + "|" + r.targetField())
                .collect(Collectors.groupingBy(k -> k, Collectors.counting()))
                .entrySet().stream()
                .filter(e -> e.getValue() > 1)
                .map(java.util.Map.Entry::getKey)
                .toList();
        assertTrue(dupKeys.isEmpty(), () -> "Duplicate INSERT_SELECT_MAP rows found: " + dupKeys);

        List<String> selectFieldDupKeys = rows.stream()
                .filter(r -> r.relationship() == RelationshipType.SELECT_FIELD)
                .map(r -> r.lineNo() + "|" + r.sourceField() + "|" + r.targetObject() + "|" + r.targetField())
                .collect(Collectors.groupingBy(k -> k, Collectors.counting()))
                .entrySet().stream()
                .filter(e -> e.getValue() > 1)
                .map(java.util.Map.Entry::getKey)
                .toList();
        assertTrue(selectFieldDupKeys.isEmpty(), () -> "Duplicate SELECT_FIELD rows found: " + selectFieldDupKeys);

        List<String> selectExprDupKeys = rows.stream()
                .filter(r -> r.relationship() == RelationshipType.SELECT_EXPR)
                .map(r -> r.lineNo() + "|" + r.sourceField() + "|" + r.targetObject() + "|" + r.targetField())
                .collect(Collectors.groupingBy(k -> k, Collectors.counting()))
                .entrySet().stream()
                .filter(e -> e.getValue() > 1)
                .map(java.util.Map.Entry::getKey)
                .toList();
        assertTrue(selectExprDupKeys.isEmpty(), () -> "Duplicate SELECT_EXPR rows found: " + selectExprDupKeys);

        List<String> whereDupKeys = rows.stream()
                .filter(r -> r.relationship() == RelationshipType.WHERE)
                .map(r -> r.lineNo() + "|" + r.sourceField() + "|" + r.targetObject() + "|" + r.targetField())
                .collect(Collectors.groupingBy(k -> k, Collectors.counting()))
                .entrySet().stream()
                .filter(e -> e.getValue() > 1)
                .map(java.util.Map.Entry::getKey)
                .toList();
        assertTrue(whereDupKeys.isEmpty(), () -> "Duplicate WHERE rows found: " + whereDupKeys);

        List<String> callParamDupKeys = rows.stream()
                .filter(r -> r.relationship() == RelationshipType.CALL_PARAM_MAP)
                .map(r -> r.lineNo() + "|" + r.sourceField() + "|" + r.targetObject() + "|" + r.targetField().trim())
                .collect(Collectors.groupingBy(k -> k, Collectors.counting()))
                .entrySet().stream()
                .filter(e -> e.getValue() > 1)
                .map(java.util.Map.Entry::getKey)
                .toList();
        assertTrue(callParamDupKeys.isEmpty(), () -> "Duplicate CALL_PARAM_MAP rows found: " + callParamDupKeys);

        List<String> variableSetDupKeys = rows.stream()
                .filter(r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP)
                .map(r -> r.lineNo() + "|" + r.sourceField() + "|" + r.targetObject() + "|" + r.targetField())
                .collect(Collectors.groupingBy(k -> k, Collectors.counting()))
                .entrySet().stream()
                .filter(e -> e.getValue() > 1)
                .map(java.util.Map.Entry::getKey)
                .toList();
        assertTrue(variableSetDupKeys.isEmpty(), () -> "Duplicate VARIABLE_SET_MAP rows found: " + variableSetDupKeys);

    }

    private static void assertHasVariableSet(List<RelationshipRow> rows, int lineNo, String targetField, String sourceField) {
        Predicate<RelationshipRow> predicate = r -> r.relationship() == RelationshipType.VARIABLE_SET_MAP
                && r.lineNo() == lineNo
                && targetField.equalsIgnoreCase(r.targetField())
                && sourceField.equalsIgnoreCase(r.sourceField());
        assertTrue(rows.stream().anyMatch(predicate),
                () -> "Missing VARIABLE_SET_MAP row line=" + lineNo + " target=" + targetField + " source=" + sourceField);
    }

    private static void assertSelectExprRowsBeforeInsertSelectMapRows(List<RelationshipRow> rows, int lineNo) {
        List<RelationshipRow> lineRows = rows.stream()
                .filter(r -> r.lineNo() == lineNo)
                .filter(r -> r.relationship() == RelationshipType.SELECT_EXPR
                        || r.relationship() == RelationshipType.INSERT_SELECT_MAP)
                .sorted(Comparator.comparingInt(RelationshipRow::lineRelationSeq))
                .toList();
        List<RelationshipRow> selectExpr = lineRows.stream()
                .filter(r -> r.relationship() == RelationshipType.SELECT_EXPR)
                .toList();
        List<RelationshipRow> insertMap = lineRows.stream()
                .filter(r -> r.relationship() == RelationshipType.INSERT_SELECT_MAP)
                .toList();
        assertFalse(selectExpr.isEmpty(), () -> "Expected SELECT_EXPR rows on line " + lineNo);
        assertFalse(insertMap.isEmpty(), () -> "Expected INSERT_SELECT_MAP rows on line " + lineNo);
        int maxSelectSeq = selectExpr.stream().mapToInt(RelationshipRow::lineRelationSeq).max().orElse(-1);
        int minInsertSeq = insertMap.stream().mapToInt(RelationshipRow::lineRelationSeq).min().orElse(Integer.MAX_VALUE);
        assertTrue(maxSelectSeq < minInsertSeq,
                () -> "Expected SELECT_EXPR rows before INSERT_SELECT_MAP rows on line " + lineNo
                        + " but got " + lineRows);
    }
}
