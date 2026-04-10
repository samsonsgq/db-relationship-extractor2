CREATE PROCEDURE RPT.PR_TEST_DEMO
(
    IN p_product_type VARCHAR(2),
    IN p_deal_type    VARCHAR(2)
)
SPECIFIC PR_TEST_DEMO
LANGUAGE SQL
RESULT SETS 0
BEGIN

    -------------------------------------------------------------------------
    -- Variable declaration
    -------------------------------------------------------------------------
    DECLARE lv_msg_category            VARCHAR(30)  DEFAULT 'ARG';
    DECLARE lv_procedure_name          VARCHAR(100) DEFAULT 'RPT.PR_TEST_DEMO';
    DECLARE lv_err_pos                 VARCHAR(1000);
    DECLARE lv_message_text            VARCHAR(1024);
    DECLARE lv_error_message           VARCHAR(3000);
    DECLARE lv_sqlstate                CHAR(5) DEFAULT '00000';
    DECLARE lv_region_code             VARCHAR(3);
    DECLARE lv_month_flag              CHAR(1);
    DECLARE lv_source_system           VARCHAR(10) DEFAULT 'STO';
    DECLARE lv_process_status          VARCHAR(20) DEFAULT 'START';
    DECLARE lv_batch_comment           VARCHAR(500);
    DECLARE lv_row_count               INTEGER DEFAULT 0;
    DECLARE lv_insert_count            INTEGER DEFAULT 0;
    DECLARE lv_update_count            INTEGER DEFAULT 0;
    DECLARE lv_merge_count             INTEGER DEFAULT 0;
    DECLARE lv_has_data                SMALLINT DEFAULT 0;

    DECLARE ld_biz_date                DATE;
    DECLARE ld_last_biz_date           DATE;
    DECLARE ld_next_biz_date           DATE;
    DECLARE ld_actual_month_begin_date DATE;
    DECLARE ld_actual_month_end_date   DATE;
    DECLARE ld_end_date                DATE;

    DECLARE ln_sqlcode                 INT DEFAULT 0;

    -------------------------------------------------------------------------
    -- Cursor variables
    -------------------------------------------------------------------------
    DECLARE cv_customer_number         VARCHAR(30);
    DECLARE cv_sub_account_number      VARCHAR(30);
    DECLARE cv_deal_number             VARCHAR(30);
    DECLARE cv_deal_sub_number         VARCHAR(30);
    DECLARE cv_event_id                VARCHAR(100);
    DECLARE cv_sequence_number         BIGINT;
    DECLARE cv_premium_amount          DECIMAL(31,10);
    DECLARE cv_settlement_amount       DECIMAL(31,10);
    DECLARE cv_trade_ccy               VARCHAR(10);
    DECLARE cv_settlement_ccy          VARCHAR(10);
    DECLARE cv_option_style            VARCHAR(10);
    DECLARE cv_expiry_date             DATE;
    DECLARE cv_exercise_date           DATE;
    DECLARE cv_knock_in_flag           CHAR(1);
    DECLARE cv_knock_out_flag          CHAR(1);

    DECLARE at_end                     SMALLINT DEFAULT 0;

    -------------------------------------------------------------------------
    -- Temporary table
    -------------------------------------------------------------------------
    DECLARE GLOBAL TEMPORARY TABLE SESSION.TMP_STO_EVENT_SOURCE
    (
        CUSTOMER_NUMBER        VARCHAR(30),
        SUB_ACCOUNT_NUMBER     VARCHAR(30),
        DEAL_NUMBER            VARCHAR(30),
        DEAL_SUB_NUMBER        VARCHAR(30),
        PRODUCT_TYPE           VARCHAR(2),
        DEAL_TYPE              VARCHAR(2),
        EVENT_CLASS            VARCHAR(10),
        EVENT_DATE             DATE,
        EVENT_ID               VARCHAR(100),
        PREMIUM_AMOUNT         DECIMAL(31,10),
        SETTLEMENT_AMOUNT      DECIMAL(31,10),
        TRADE_CCY              VARCHAR(10),
        SETTLEMENT_CCY         VARCHAR(10),
        OPTION_STYLE           VARCHAR(10),
        STRIKE_PRICE           DECIMAL(31,10),
        NOTIONAL_AMOUNT        DECIMAL(31,10),
        KNOCK_IN_FLAG          CHAR(1),
        KNOCK_OUT_FLAG         CHAR(1),
        EXPIRY_DATE            DATE,
        EXERCISE_DATE          DATE,
        COMPANY_CODE           VARCHAR(20),
        BRANCH_CODE            VARCHAR(20)
    )
    WITH REPLACE
    ON COMMIT PRESERVE ROWS
    NOT LOGGED;

    -------------------------------------------------------------------------
    -- Exception handling
    -------------------------------------------------------------------------
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET ln_sqlcode = SQLCODE;
        SET lv_sqlstate = SQLSTATE;

        GET DIAGNOSTICS EXCEPTION 1 lv_message_text = MESSAGE_TEXT;
        GET DIAGNOSTICS lv_row_count = ROW_COUNT;

        ROLLBACK;

        SET lv_error_message =
              lv_procedure_name || ' failed.'
           || ' Product Type: ' || COALESCE(p_product_type, '')
           || ' Deal Type: '    || COALESCE(p_deal_type, '')
           || ' Err Pos: '      || COALESCE(lv_err_pos, '')
           || ' Message: '      || COALESCE(lv_message_text, '')
           || ' SQLSTATE: '     || COALESCE(lv_sqlstate, '')
           || ' SQLCODE: '      || CHAR(ln_sqlcode)
           || ' ROW_COUNT: '    || CHAR(lv_row_count);

        CALL TEMP.PR_PROCEDURE_LOG(
            lv_msg_category,
            lv_procedure_name,
            'ERROR',
            lv_error_message
        );

        COMMIT;
    END;

    -------------------------------------------------------------------------
    -- Continue handler for cursor end
    -------------------------------------------------------------------------
    DECLARE CONTINUE HANDLER FOR NOT FOUND
        SET at_end = 1;

    -------------------------------------------------------------------------
    -- Cursor declaration
    -------------------------------------------------------------------------
    DECLARE c_event_detail CURSOR FOR
        SELECT MAST.CUSTOMER_NUMBER,
               MAST.SUB_ACCOUNT_NUMBER,
               MAST.DEAL_NUMBER,
               MAST.DEAL_SUB_NUMBER,
               MAST.EVENT_ID,
               MAST.SEQUENCE_NUMBER,
               SRC.PREMIUM_AMOUNT,
               SRC.SETTLEMENT_AMOUNT,
               SRC.TRADE_CCY,
               SRC.SETTLEMENT_CCY,
               SRC.OPTION_STYLE,
               SRC.EXPIRY_DATE,
               SRC.EXERCISE_DATE,
               SRC.KNOCK_IN_FLAG,
               SRC.KNOCK_OUT_FLAG
          FROM TEMP.TEST_EVENT_MASTER_P MAST
          INNER JOIN SESSION.TMP_STO_EVENT_SOURCE SRC
            ON SRC.CUSTOMER_NUMBER    = MAST.CUSTOMER_NUMBER
           AND SRC.SUB_ACCOUNT_NUMBER = MAST.SUB_ACCOUNT_NUMBER
           AND SRC.DEAL_NUMBER        = MAST.DEAL_NUMBER
           AND SRC.DEAL_SUB_NUMBER    = MAST.DEAL_SUB_NUMBER
           AND SRC.EVENT_ID           = MAST.EVENT_ID
         WHERE MAST.PRODUCT_TYPE      = p_product_type
           AND MAST.DEAL_TYPE         = p_deal_type
           AND MAST.CREATION_DATE     = ld_biz_date
           AND MAST.EVENT_ID LIKE 'OTC_STO_%';

    -------------------------------------------------------------------------
    -- Start log
    -------------------------------------------------------------------------
    CALL TEMP.PR_PROCEDURE_LOG(
        lv_msg_category,
        lv_procedure_name,
        'INFO',
        'Start CALL RPT.PR_TEST_DEMO()'
    );

    -------------------------------------------------------------------------
    -- Get business date / parameters
    -------------------------------------------------------------------------
    SET lv_err_pos = 'Position: Get business date and control parameters';

    SELECT BIZ_DT,
           PREV_BIZ_DT,
           NEXT_BIZ_DT,
           MONTH_FLAG,
           REGION_CD
      INTO ld_biz_date,
           ld_last_biz_date,
           ld_next_biz_date,
           lv_month_flag,
           lv_region_code
      FROM TEMP.SYS_WHERE
     WHERE SYS_CD = 'FOS';

    SET ld_actual_month_begin_date = TEMP.FN_GET_ACTUAL_MONTH_BEGIN_DATE();
    SET ld_actual_month_end_date   = TEMP.FN_GET_ACTUAL_MONTH_END_DATE();

    SET ld_end_date =
        CASE
            WHEN lv_month_flag = 'E' THEN ld_actual_month_end_date
            ELSE ld_biz_date
        END;

    SET lv_batch_comment =
          'biz_date=' || CHAR(ld_biz_date)
       || ', prev_biz_date=' || CHAR(ld_last_biz_date)
       || ', next_biz_date=' || CHAR(ld_next_biz_date)
       || ', region=' || COALESCE(lv_region_code, '')
       || ', month_flag=' || COALESCE(lv_month_flag, '');

    CALL TEMP.PR_PROCEDURE_LOG(
        lv_msg_category,
        lv_procedure_name,
        'INFO',
        lv_batch_comment
    );

    -------------------------------------------------------------------------
    -- Clean temp table
    -------------------------------------------------------------------------
    SET lv_err_pos = 'Position: Clean temp table';

    DELETE FROM SESSION.TMP_STO_EVENT_SOURCE;

    -------------------------------------------------------------------------
    -- Clear old data for rerun
    -------------------------------------------------------------------------
    SET lv_err_pos = 'Position: Clear rerun data';

    DELETE FROM TEMP.TEST_EVENT_DETAIL_P DETL
     WHERE DETL.PRODUCT_TYPE   = p_product_type
       AND DETL.DEAL_TYPE      = p_deal_type
       AND DETL.CREATION_DATE  = ld_biz_date
       AND DETL.EVENT_ID LIKE 'OTC_STO_%';

    DELETE FROM TEMP.TEST_EVENT_MASTER_P MAST
     WHERE MAST.PRODUCT_TYPE   = p_product_type
       AND MAST.DEAL_TYPE      = p_deal_type
       AND MAST.CREATION_DATE  = ld_biz_date
       AND MAST.EVENT_ID LIKE 'OTC_STO_%';

    COMMIT;

    CALL TEMP.PR_PROCEDURE_LOG(
        lv_msg_category,
        lv_procedure_name,
        'INFO',
        'Old rerun data deleted'
    );

    -------------------------------------------------------------------------
    -- Load source events into temp table
    -------------------------------------------------------------------------
    SET lv_err_pos = 'Position: Load source event rows into temp table';

    INSERT INTO SESSION.TMP_STO_EVENT_SOURCE
    (
        CUSTOMER_NUMBER,
        SUB_ACCOUNT_NUMBER,
        DEAL_NUMBER,
        DEAL_SUB_NUMBER,
        PRODUCT_TYPE,
        DEAL_TYPE,
        EVENT_CLASS,
        EVENT_DATE,
        EVENT_ID,
        PREMIUM_AMOUNT,
        SETTLEMENT_AMOUNT,
        TRADE_CCY,
        SETTLEMENT_CCY,
        OPTION_STYLE,
        STRIKE_PRICE,
        NOTIONAL_AMOUNT,
        KNOCK_IN_FLAG,
        KNOCK_OUT_FLAG,
        EXPIRY_DATE,
        EXERCISE_DATE,
        COMPANY_CODE,
        BRANCH_CODE
    )
    SELECT DEAL.CUSTOMER_NUMBER,
           DEAL.SUB_ACCOUNT_NUMBER,
           DEAL.DEAL_NUMBER,
           DEAL.DEAL_SUB_NUMBER,
           DEAL.PROD_TYPE,
           DEAL.DEAL_TYPE,
           CASE
               WHEN DEAL.TRADE_DATE = ld_biz_date THEN 'TRADE'
               WHEN DEAL.PREMIUM_DATE = ld_biz_date THEN 'PREMIUM'
               WHEN DEAL.EXERCISE_DATE = ld_biz_date AND DEAL.EXERCISED_FLAG = 'Y' THEN 'EXERCISE'
               WHEN DEAL.EXPIRY_DATE = ld_biz_date AND COALESCE(DEAL.EXERCISED_FLAG, 'N') = 'N' THEN 'EXPIRY'
               WHEN DEAL.MATURITY_DATE = ld_biz_date THEN 'MATURITY'
               ELSE 'OTHER'
           END,
           CASE
               WHEN DEAL.TRADE_DATE = ld_biz_date THEN DEAL.TRADE_DATE
               WHEN DEAL.PREMIUM_DATE = ld_biz_date THEN DEAL.PREMIUM_DATE
               WHEN DEAL.EXERCISE_DATE = ld_biz_date THEN DEAL.EXERCISE_DATE
               WHEN DEAL.EXPIRY_DATE = ld_biz_date THEN DEAL.EXPIRY_DATE
               ELSE DEAL.MATURITY_DATE
           END,
           CASE
               WHEN DEAL.TRADE_DATE = ld_biz_date AND DEAL.BUY_SELL_FLAG = 'B'
                    THEN 'OTC_STO_PURCHASE_TRADE_DATE'
               WHEN DEAL.TRADE_DATE = ld_biz_date AND DEAL.BUY_SELL_FLAG = 'S'
                    THEN 'OTC_STO_WRITTEN_TRADE_DATE'
               WHEN DEAL.PREMIUM_DATE = ld_biz_date AND DEAL.BUY_SELL_FLAG = 'B'
                    THEN 'OTC_STO_PURCHASE_PREMIUM_DATE'
               WHEN DEAL.PREMIUM_DATE = ld_biz_date AND DEAL.BUY_SELL_FLAG = 'S'
                    THEN 'OTC_STO_WRITTEN_PREMIUM_DATE'
               WHEN DEAL.EXERCISE_DATE = ld_biz_date AND DEAL.EXERCISED_FLAG = 'Y'
                    THEN 'OTC_STO_EXERCISE_DATE'
               WHEN DEAL.EXPIRY_DATE = ld_biz_date AND COALESCE(DEAL.EXERCISED_FLAG, 'N') = 'N'
                    THEN 'OTC_STO_EXPIRY_DATE'
               WHEN DEAL.MATURITY_DATE = ld_biz_date AND DEAL.BUY_SELL_FLAG = 'B'
                    THEN 'OTC_STO_PURCHASE_MATURITY_DATE'
               ELSE 'OTC_STO_WRITTEN_MATURITY_DATE'
           END,
           DEAL.PREMIUM_AMOUNT,
           CASE
               WHEN POSN.CLOSE_OUT_AMOUNT IS NOT NULL THEN POSN.CLOSE_OUT_AMOUNT
               WHEN DEAL.SETTLEMENT_AMOUNT IS NOT NULL THEN DEAL.SETTLEMENT_AMOUNT
               ELSE 0
           END,
           DEAL.TRADE_CCY,
           DEAL.SETTLEMENT_CCY,
           DEAL.OPTION_STYLE,
           DEAL.STRIKE_PRICE,
           DEAL.NOTIONAL_AMOUNT,
           COALESCE(DEAL.KNOCK_IN_FLAG, 'N'),
           COALESCE(DEAL.KNOCK_OUT_FLAG, 'N'),
           DEAL.EXPIRY_DATE,
           DEAL.EXERCISE_DATE,
           DEAL.COMPANY_CODE,
           DEAL.BRANCH_CODE
      FROM TEMP.STRUCTURED_OPTION_DEAL_BEF_EOD DEAL
      LEFT OUTER JOIN TEMP.OTC_STO_POSITION POSN
        ON POSN.CUSTOMER_NUMBER    = DEAL.CUSTOMER_NUMBER
       AND POSN.SUB_ACCOUNT_NUMBER = DEAL.SUB_ACCOUNT_NUMBER
       AND POSN.DEAL_TYPE          = DEAL.DEAL_TYPE
       AND POSN.DEAL_NUMBER        = DEAL.DEAL_NUMBER
       AND POSN.DEAL_SUB_NUMBER    = DEAL.DEAL_SUB_NUMBER
      LEFT OUTER JOIN TEMP.STRUCTURED_OPTION_TEMPLATE TMPL
        ON TMPL.TEMPLATE_CODE      = DEAL.TEMPLATE_CODE
     WHERE DEAL.PROD_TYPE          = p_product_type
       AND DEAL.DEAL_TYPE          = p_deal_type
       AND DEAL.REVERSE_TS IS NULL
       AND DEAL.STATUS IN ('A', 'L', 'P')
       AND TMPL.TEMPLATE_CODE IS NOT NULL
       AND
       (
            DEAL.TRADE_DATE     = ld_biz_date
         OR DEAL.PREMIUM_DATE   = ld_biz_date
         OR DEAL.EXERCISE_DATE  = ld_biz_date
         OR DEAL.EXPIRY_DATE    = ld_biz_date
         OR DEAL.MATURITY_DATE  = ld_biz_date
       );

    GET DIAGNOSTICS lv_row_count = ROW_COUNT;
    SET lv_insert_count = lv_insert_count + lv_row_count;

    COMMIT;

    -------------------------------------------------------------------------
    -- Check if data exists
    -------------------------------------------------------------------------
    SET lv_err_pos = 'Position: Check temp source data';

    SELECT CASE WHEN EXISTS (SELECT 1 FROM SESSION.TMP_STO_EVENT_SOURCE) THEN 1 ELSE 0 END
      INTO lv_has_data
      FROM SYSIBM.SYSDUMMY1;

    IF lv_has_data = 0 THEN

        CALL TEMP.PR_PROCEDURE_LOG(
            lv_msg_category,
            lv_procedure_name,
            'INFO',
            'No source rows found for structured option events'
        );

    ELSE

        ---------------------------------------------------------------------
        -- Insert ACCOUNT_EVENT_MASTER_P
        ---------------------------------------------------------------------
        SET lv_err_pos = 'Position: Insert ACCOUNT_EVENT_MASTER_P';

        INSERT INTO TEMP.TEST_EVENT_MASTER_P
        (
            ACTIVE_FLAG,
            CREATION_DATE,
            AS_OF_DATE,
            PRODUCT_TYPE,
            DEAL_TYPE,
            EVENT_ID,
            SEQUENCE_NUMBER,
            DEAL_WITH_COMPANY,
            DEAL_WITH_BRANCH,
            CUSTOMER_NUMBER,
            SUB_ACCOUNT_NUMBER,
            DEAL_NUMBER,
            DEAL_SUB_NUMBER
        )
        SELECT 'A',
               ld_biz_date,
               ld_biz_date,
               SRC.PRODUCT_TYPE,
               SRC.DEAL_TYPE,
               SRC.EVENT_ID,
               NEXT VALUE FOR RPT.GLOBAL_SEQ_ACCOUNT_EVENT_UNIQUE_ID,
               SRC.COMPANY_CODE,
               SRC.BRANCH_CODE,
               SRC.CUSTOMER_NUMBER,
               SRC.SUB_ACCOUNT_NUMBER,
               SRC.DEAL_NUMBER,
               SRC.DEAL_SUB_NUMBER
          FROM SESSION.TMP_STO_EVENT_SOURCE SRC;

        GET DIAGNOSTICS lv_row_count = ROW_COUNT;
        SET lv_insert_count = lv_insert_count + lv_row_count;

        COMMIT;

        ---------------------------------------------------------------------
        -- Insert static detail rows
        ---------------------------------------------------------------------
        SET lv_err_pos = 'Position: Insert static detail rows';

        INSERT INTO TEMP.TEST_EVENT_DETAIL_P
        (
            ACTIVE_FLAG,
            CREATION_DATE,
            AS_OF_DATE,
            PRODUCT_TYPE,
            DEAL_TYPE,
            EVENT_ID,
            SEQUENCE_NUMBER,
            FIELD_NAME,
            FIELD_VALUE
        )
        SELECT MAST.ACTIVE_FLAG,
               ld_biz_date,
               MAST.AS_OF_DATE,
               MAST.PRODUCT_TYPE,
               MAST.DEAL_TYPE,
               MAST.EVENT_ID,
               MAST.SEQUENCE_NUMBER,
               'SOURCE_SYSTEM',
               lv_source_system
          FROM TEMP.TEST_EVENT_MASTER_P MAST
         WHERE MAST.PRODUCT_TYPE  = p_product_type
           AND MAST.DEAL_TYPE     = p_deal_type
           AND MAST.CREATION_DATE = ld_biz_date
           AND MAST.EVENT_ID LIKE 'OTC_STO_%';

        GET DIAGNOSTICS lv_row_count = ROW_COUNT;
        SET lv_insert_count = lv_insert_count + lv_row_count;

        INSERT INTO TEMP.TEST_EVENT_DETAIL_P
        (
            ACTIVE_FLAG,
            CREATION_DATE,
            AS_OF_DATE,
            PRODUCT_TYPE,
            DEAL_TYPE,
            EVENT_ID,
            SEQUENCE_NUMBER,
            FIELD_NAME,
            FIELD_VALUE
        )
        SELECT MAST.ACTIVE_FLAG,
               ld_biz_date,
               MAST.AS_OF_DATE,
               MAST.PRODUCT_TYPE,
               MAST.DEAL_TYPE,
               MAST.EVENT_ID,
               MAST.SEQUENCE_NUMBER,
               'REGION_CODE',
               COALESCE(lv_region_code, '')
          FROM TEMP.TEST_EVENT_MASTER_P MAST
         WHERE MAST.PRODUCT_TYPE  = p_product_type
           AND MAST.DEAL_TYPE     = p_deal_type
           AND MAST.CREATION_DATE = ld_biz_date
           AND MAST.EVENT_ID LIKE 'OTC_STO_%';

        GET DIAGNOSTICS lv_row_count = ROW_COUNT;
        SET lv_insert_count = lv_insert_count + lv_row_count;

        COMMIT;

        ---------------------------------------------------------------------
        -- Insert detail rows using cursor
        ---------------------------------------------------------------------
        SET lv_err_pos = 'Position: Insert dynamic detail rows by cursor';

        SET at_end = 0;
        OPEN c_event_detail;

        fetch_loop:
        LOOP
            FETCH c_event_detail
             INTO cv_customer_number,
                  cv_sub_account_number,
                  cv_deal_number,
                  cv_deal_sub_number,
                  cv_event_id,
                  cv_sequence_number,
                  cv_premium_amount,
                  cv_settlement_amount,
                  cv_trade_ccy,
                  cv_settlement_ccy,
                  cv_option_style,
                  cv_expiry_date,
                  cv_exercise_date,
                  cv_knock_in_flag,
                  cv_knock_out_flag;

            IF at_end = 1 THEN
                LEAVE fetch_loop;
            END IF;

            INSERT INTO TEMP.TEST_EVENT_DETAIL_P
            (
                ACTIVE_FLAG,
                CREATION_DATE,
                AS_OF_DATE,
                PRODUCT_TYPE,
                DEAL_TYPE,
                EVENT_ID,
                SEQUENCE_NUMBER,
                FIELD_NAME,
                FIELD_VALUE
            )
            VALUES
            (
                'A',
                ld_biz_date,
                ld_biz_date,
                p_product_type,
                p_deal_type,
                cv_event_id,
                cv_sequence_number,
                'PREMIUM_AMOUNT',
                COALESCE(CHAR(cv_premium_amount), '0')
            );

            INSERT INTO TEMP.TEST_EVENT_DETAIL_P
            (
                ACTIVE_FLAG,
                CREATION_DATE,
                AS_OF_DATE,
                PRODUCT_TYPE,
                DEAL_TYPE,
                EVENT_ID,
                SEQUENCE_NUMBER,
                FIELD_NAME,
                FIELD_VALUE
            )
            VALUES
            (
                'A',
                ld_biz_date,
                ld_biz_date,
                p_product_type,
                p_deal_type,
                cv_event_id,
                cv_sequence_number,
                'SETTLEMENT_AMOUNT',
                COALESCE(CHAR(cv_settlement_amount), '0')
            );

            INSERT INTO TEMP.TEST_EVENT_DETAIL_P
            (
                ACTIVE_FLAG,
                CREATION_DATE,
                AS_OF_DATE,
                PRODUCT_TYPE,
                DEAL_TYPE,
                EVENT_ID,
                SEQUENCE_NUMBER,
                FIELD_NAME,
                FIELD_VALUE
            )
            VALUES
            (
                'A',
                ld_biz_date,
                ld_biz_date,
                p_product_type,
                p_deal_type,
                cv_event_id,
                cv_sequence_number,
                'TRADE_CCY',
                COALESCE(cv_trade_ccy, '')
            );

            INSERT INTO TEMP.TEST_EVENT_DETAIL_P
            (
                ACTIVE_FLAG,
                CREATION_DATE,
                AS_OF_DATE,
                PRODUCT_TYPE,
                DEAL_TYPE,
                EVENT_ID,
                SEQUENCE_NUMBER,
                FIELD_NAME,
                FIELD_VALUE
            )
            VALUES
            (
                'A',
                ld_biz_date,
                ld_biz_date,
                p_product_type,
                p_deal_type,
                cv_event_id,
                cv_sequence_number,
                'SETTLEMENT_CCY',
                COALESCE(cv_settlement_ccy, '')
            );

            INSERT INTO TEMP.TEST_EVENT_DETAIL_P
            (
                ACTIVE_FLAG,
                CREATION_DATE,
                AS_OF_DATE,
                PRODUCT_TYPE,
                DEAL_TYPE,
                EVENT_ID,
                SEQUENCE_NUMBER,
                FIELD_NAME,
                FIELD_VALUE
            )
            VALUES
            (
                'A',
                ld_biz_date,
                ld_biz_date,
                p_product_type,
                p_deal_type,
                cv_event_id,
                cv_sequence_number,
                'OPTION_STYLE',
                COALESCE(cv_option_style, '')
            );

            INSERT INTO TEMP.TEST_EVENT_DETAIL_P
            (
                ACTIVE_FLAG,
                CREATION_DATE,
                AS_OF_DATE,
                PRODUCT_TYPE,
                DEAL_TYPE,
                EVENT_ID,
                SEQUENCE_NUMBER,
                FIELD_NAME,
                FIELD_VALUE
            )
            VALUES
            (
                'A',
                ld_biz_date,
                ld_biz_date,
                p_product_type,
                p_deal_type,
                cv_event_id,
                cv_sequence_number,
                'KNOCK_IN_FLAG',
                COALESCE(cv_knock_in_flag, 'N')
            );

            INSERT INTO TEMP.TEST_EVENT_DETAIL_P
            (
                ACTIVE_FLAG,
                CREATION_DATE,
                AS_OF_DATE,
                PRODUCT_TYPE,
                DEAL_TYPE,
                EVENT_ID,
                SEQUENCE_NUMBER,
                FIELD_NAME,
                FIELD_VALUE
            )
            VALUES
            (
                'A',
                ld_biz_date,
                ld_biz_date,
                p_product_type,
                p_deal_type,
                cv_event_id,
                cv_sequence_number,
                'KNOCK_OUT_FLAG',
                COALESCE(cv_knock_out_flag, 'N')
            );

            INSERT INTO TEMP.TEST_EVENT_DETAIL_P
            (
                ACTIVE_FLAG,
                CREATION_DATE,
                AS_OF_DATE,
                PRODUCT_TYPE,
                DEAL_TYPE,
                EVENT_ID,
                SEQUENCE_NUMBER,
                FIELD_NAME,
                FIELD_VALUE
            )
            VALUES
            (
                'A',
                ld_biz_date,
                ld_biz_date,
                p_product_type,
                p_deal_type,
                cv_event_id,
                cv_sequence_number,
                'EXPIRY_DATE',
                CASE WHEN cv_expiry_date IS NULL THEN '' ELSE CHAR(cv_expiry_date) END
            );

            INSERT INTO TEMP.TEST_EVENT_DETAIL_P
            (
                ACTIVE_FLAG,
                CREATION_DATE,
                AS_OF_DATE,
                PRODUCT_TYPE,
                DEAL_TYPE,
                EVENT_ID,
                SEQUENCE_NUMBER,
                FIELD_NAME,
                FIELD_VALUE
            )
            VALUES
            (
                'A',
                ld_biz_date,
                ld_biz_date,
                p_product_type,
                p_deal_type,
                cv_event_id,
                cv_sequence_number,
                'EXERCISE_DATE',
                CASE WHEN cv_exercise_date IS NULL THEN '' ELSE CHAR(cv_exercise_date) END
            );

            SET lv_insert_count = lv_insert_count + 9;
        END LOOP;

        CLOSE c_event_detail;

        COMMIT;

        ---------------------------------------------------------------------
        -- Update original deal number and deal sub number from SOD
        ---------------------------------------------------------------------
        SET lv_err_pos = 'Position: Update original deal number from SOD';

        UPDATE TEMP.TEST_EVENT_MASTER_P MAST
           SET (MAST.DEAL_NUMBER, MAST.DEAL_SUB_NUMBER) =
               (
                   SELECT SOD.ORIG_DEAL_NUM,
                          SOD.ORIG_DEAL_SB_NUM
                     FROM TEMP.STRUCTURED_OPTION_SOD SOD
                    WHERE SOD.CUST_NUM    = MAST.CUSTOMER_NUMBER
                      AND SOD.SB_ACCT_NUM = MAST.SUB_ACCOUNT_NUMBER
                      AND SOD.DEAL_NUM    = MAST.DEAL_NUMBER
                      AND SOD.DEAL_SB_NUM = MAST.DEAL_SUB_NUMBER
                    FETCH FIRST 1 ROW ONLY
               )
         WHERE MAST.PRODUCT_TYPE  = p_product_type
           AND MAST.DEAL_TYPE     = p_deal_type
           AND MAST.CREATION_DATE = ld_biz_date
           AND MAST.EVENT_ID LIKE 'OTC_STO_%'
           AND EXISTS
               (
                   SELECT 1
                     FROM TEMP.STRUCTURED_OPTION_SOD SOD
                    WHERE SOD.CUST_NUM    = MAST.CUSTOMER_NUMBER
                      AND SOD.SB_ACCT_NUM = MAST.SUB_ACCOUNT_NUMBER
                      AND SOD.DEAL_NUM    = MAST.DEAL_NUMBER
                      AND SOD.DEAL_SB_NUM = MAST.DEAL_SUB_NUMBER
               );

        GET DIAGNOSTICS lv_row_count = ROW_COUNT;
        SET lv_update_count = lv_update_count + lv_row_count;

        COMMIT;

        ---------------------------------------------------------------------
        -- Merge summary information into summary table
        ---------------------------------------------------------------------
        SET lv_err_pos = 'Position: Merge into summary table';

        MERGE INTO TEMP.TEST_EVENT_DAILY_SUMMARY T
        USING
        (
            SELECT ld_biz_date AS BIZ_DATE,
                   p_product_type AS PRODUCT_TYPE,
                   p_deal_type AS DEAL_TYPE,
                   COUNT(*) AS EVENT_COUNT
              FROM TEMP.TEST_EVENT_MASTER_P MAST
             WHERE MAST.CREATION_DATE = ld_biz_date
               AND MAST.PRODUCT_TYPE  = p_product_type
               AND MAST.DEAL_TYPE     = p_deal_type
               AND MAST.EVENT_ID LIKE 'OTC_STO_%'
        ) S
        ON T.BIZ_DATE      = S.BIZ_DATE
       AND T.PRODUCT_TYPE  = S.PRODUCT_TYPE
       AND T.DEAL_TYPE     = S.DEAL_TYPE
        WHEN MATCHED THEN
            UPDATE SET
                T.EVENT_COUNT  = S.EVENT_COUNT,
                T.UPDATE_TS    = CURRENT TIMESTAMP,
                T.UPDATE_USER  = lv_procedure_name
        WHEN NOT MATCHED THEN
            INSERT
            (
                BIZ_DATE,
                PRODUCT_TYPE,
                DEAL_TYPE,
                EVENT_COUNT,
                CREATE_TS,
                CREATE_USER
            )
            VALUES
            (
                S.BIZ_DATE,
                S.PRODUCT_TYPE,
                S.DEAL_TYPE,
                S.EVENT_COUNT,
                CURRENT TIMESTAMP,
                lv_procedure_name
            );

        GET DIAGNOSTICS lv_row_count = ROW_COUNT;
        SET lv_merge_count = lv_merge_count + lv_row_count;

        COMMIT;

        ---------------------------------------------------------------------
        -- Additional detail rows for derived business flags
        ---------------------------------------------------------------------
        SET lv_err_pos = 'Position: Insert derived business detail rows';

        INSERT INTO TEMP.TEST_EVENT_DETAIL_P
        (
            ACTIVE_FLAG,
            CREATION_DATE,
            AS_OF_DATE,
            PRODUCT_TYPE,
            DEAL_TYPE,
            EVENT_ID,
            SEQUENCE_NUMBER,
            FIELD_NAME,
            FIELD_VALUE
        )
        SELECT 'A',
               ld_biz_date,
               ld_biz_date,
               p_product_type,
               p_deal_type,
               MAST.EVENT_ID,
               MAST.SEQUENCE_NUMBER,
               'MONTH_END_FLAG',
               CASE
                   WHEN lv_month_flag = 'E' THEN 'Y'
                   ELSE 'N'
               END
          FROM TEMP.TEST_EVENT_MASTER_P MAST
         WHERE MAST.CREATION_DATE = ld_biz_date
           AND MAST.PRODUCT_TYPE  = p_product_type
           AND MAST.DEAL_TYPE     = p_deal_type
           AND MAST.EVENT_ID LIKE 'OTC_STO_%';

        GET DIAGNOSTICS lv_row_count = ROW_COUNT;
        SET lv_insert_count = lv_insert_count + lv_row_count;

        INSERT INTO TEMP.TEST_EVENT_DETAIL_P
        (
            ACTIVE_FLAG,
            CREATION_DATE,
            AS_OF_DATE,
            PRODUCT_TYPE,
            DEAL_TYPE,
            EVENT_ID,
            SEQUENCE_NUMBER,
            FIELD_NAME,
            FIELD_VALUE
        )
        SELECT 'A',
               ld_biz_date,
               ld_biz_date,
               p_product_type,
               p_deal_type,
               MAST.EVENT_ID,
               MAST.SEQUENCE_NUMBER,
               'PROCESS_STATUS',
               lv_process_status
          FROM TEMP.TEST_EVENT_MASTER_P MAST
         WHERE MAST.CREATION_DATE = ld_biz_date
           AND MAST.PRODUCT_TYPE  = p_product_type
           AND MAST.DEAL_TYPE     = p_deal_type
           AND MAST.EVENT_ID LIKE 'OTC_STO_%';

        GET DIAGNOSTICS lv_row_count = ROW_COUNT;
        SET lv_insert_count = lv_insert_count + lv_row_count;

        COMMIT;

    END IF;

    -------------------------------------------------------------------------
    -- End log
    -------------------------------------------------------------------------
    SET lv_batch_comment =
          'Completed. inserted=' || CHAR(lv_insert_count)
       || ', updated=' || CHAR(lv_update_count)
       || ', merged=' || CHAR(lv_merge_count);

    CALL TEMP.PR_PROCEDURE_LOG(
        lv_msg_category,
        lv_procedure_name,
        'INFO',
        lv_batch_comment
    );

    CALL TEMP.PR_PROCEDURE_LOG(
        lv_msg_category,
        lv_procedure_name,
        'INFO',
        'End CALL RPT.PR_TEST_DEMO()'
    );

END
@
