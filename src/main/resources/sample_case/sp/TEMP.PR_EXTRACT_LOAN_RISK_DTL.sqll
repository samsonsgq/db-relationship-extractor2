CREATE PROCEDURE TEMP.PR_EXTRACT_LOAN_RISK_DTL ()
LANGUAGE SQL
SPECIFIC PR_EXTRACT_LOAN_RISK_DTL
BEGIN

    /***************************************************************
    * 
    * ALL RIGHTS RESERVED.
    *
    * Procedure Name : TEMP.PR_EXTRACT_LOAN_RISK_DTL
    * Purpose        : Extract daily loan risk detail report
    * Author         : ChatGPT
    * Created On     : 2026-04-10
    *
    * Amendment History:
    * --------------------------------------------------------------
    * Amended By   Amended On   Description
    * -----------  -----------  ------------------------------------
    * ChatGPT      10 Apr 2026  Initial version
    ***************************************************************/

    ----------------------------------------------------------------
    -- Variable declarations
    ----------------------------------------------------------------
    DECLARE l_report_date           DATE;
    DECLARE l_next_business_date    DATE;
    DECLARE l_country_code          CHAR(2);
    DECLARE l_site_code             CHAR(5) DEFAULT '00000';
    DECLARE l_reporting_ccy         VARCHAR(3);
    DECLARE l_is_hk                 CHAR(1);
    DECLARE l_is_sg                 CHAR(1);
    DECLARE l_batch_id              VARCHAR(30);
    DECLARE l_sql_str               VARCHAR(2000);
    DECLARE l_ctrl_flag             CHAR(1) DEFAULT 'N';
    DECLARE l_row_cnt               INTEGER DEFAULT 0;
    DECLARE l_warn_msg              VARCHAR(1000);
    DECLARE l_err_msg               VARCHAR(1000);

    ----------------------------------------------------------------
    -- Exception handling
    ----------------------------------------------------------------
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS EXCEPTION 1 l_err_msg = MESSAGE_TEXT;

        CALL TEMP.PR_PROCEDURE_LOG(
              'FORMAT'
            , 'TEMP.PR_EXTRACT_LOAN_RISK_DTL'
            , 'ERROR'
            , COALESCE(l_err_msg, 'UNKNOWN SQL EXCEPTION')
        );

        ROLLBACK;
        RESIGNAL;
    END;

    DECLARE CONTINUE HANDLER FOR NOT FOUND
    BEGIN
        SET l_warn_msg = 'NOT FOUND CONDITION ENCOUNTERED';
        CALL TEMP.PR_PROCEDURE_LOG(
              'FORMAT'
            , 'TEMP.PR_EXTRACT_LOAN_RISK_DTL'
            , 'WARN'
            , l_warn_msg
        );
    END;

    ----------------------------------------------------------------
    -- Initialize runtime variables
    ----------------------------------------------------------------
    SET l_report_date        = CURRENT DATE - 1 DAY;
    SET l_next_business_date = CURRENT DATE;
    SET l_country_code       = 'HK';
    SET l_reporting_ccy      = 'HKD';
    SET l_is_hk              = 'Y';
    SET l_is_sg              = 'N';
    SET l_batch_id           = VARCHAR_FORMAT(CURRENT TIMESTAMP, 'YYYYMMDDHH24MISS');

    CALL TEMP.PR_PROCEDURE_LOG(
          'FORMAT'
        , 'TEMP.PR_EXTRACT_LOAN_RISK_DTL'
        , 'INFO'
        , 'Procedure started. Batch=' || l_batch_id
    );

    ----------------------------------------------------------------
    -- Read control / config flag
    ----------------------------------------------------------------
    SELECT COALESCE(MAX(ctrl_flag), 'N')
      INTO l_ctrl_flag
      FROM CFG.RPT_CONTROL
     WHERE process_name = 'PR_EXTRACT_LOAN_RISK_DTL';

    ----------------------------------------------------------------
    -- Clear target table
    ----------------------------------------------------------------
    SET l_sql_str =
        'ALTER TABLE RPT.RPT_LOAN_RISK_DTL ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE';

    EXECUTE IMMEDIATE l_sql_str;
    COMMIT;

    CALL TEMP.PR_PROCEDURE_LOG(
          'FORMAT'
        , 'TEMP.PR_EXTRACT_LOAN_RISK_DTL'
        , 'INFO'
        , 'Target table cleared'
    );

    ----------------------------------------------------------------
    -- Temporary table: base loan data
    ----------------------------------------------------------------
    DECLARE GLOBAL TEMPORARY TABLE SESSION.TMP_LOAN_BASE
    (
        CUST_NUM                VARCHAR(20)     NOT NULL,
        ACCT_NUM                VARCHAR(20)     NOT NULL,
        DEAL_NUM                DECIMAL(20,0)   NOT NULL,
        DEAL_SUB_NUM            DECIMAL(10,0)   NOT NULL,
        DEAL_TYPE               VARCHAR(5)      NOT NULL,
        LOAN_CCY                VARCHAR(3)      NOT NULL,
        CURR_BAL                DECIMAL(18,2),
        BREAK_INT_AMT           DECIMAL(18,2),
        MATURITY_DATE           DATE,
        RESP_COMP_CDE           VARCHAR(5),
        COUNTRY_CODE            CHAR(2),
        GUARANTEE_IND           CHAR(1),
        RISK_RATING             DECIMAL(10,4),
        PD                      DECIMAL(10,6),
        LGD                     DECIMAL(10,6),
        SOURCE_SYSTEM_ID        VARCHAR(20)
    )
    ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE;

    INSERT INTO SESSION.TMP_LOAN_BASE
    SELECT
          l.CUST_NUM
        , l.ACCT_NUM
        , l.DEAL_NUM
        , l.DEAL_SUB_NUM
        , l.DEAL_TYPE
        , l.LOAN_CCY
        , l.CURR_BAL
        , COALESCE(l.BREAK_INT_AMT, 0)
        , l.MATURITY_DATE
        , l.RESP_COMP_CDE
        , l.COUNTRY_CODE
        , CASE WHEN g.DEAL_NUM IS NOT NULL THEN 'Y' ELSE 'N' END AS GUARANTEE_IND
        , COALESCE(rr.CUST_RISK_RATING, 0)
        , COALESCE(rr.PD, 0)
        , COALESCE(rr.LGD, 0)
        , l.SOURCE_SYSTEM_ID
    FROM INTERFACE.LOAN_DEAL l
    LEFT JOIN INTERFACE.GUARANTEE_DEAL g
           ON l.CUST_NUM     = g.CUST_NUM
          AND l.ACCT_NUM     = g.ACCT_NUM
          AND l.DEAL_NUM     = g.DEAL_NUM
          AND l.DEAL_SUB_NUM = g.DEAL_SUB_NUM
    LEFT JOIN INTERFACE.CUST_RISK_RATING rr
           ON l.CUST_NUM = rr.CUST_NUM
    WHERE l.ACTV_FLAG = 'A'
      AND COALESCE(l.CURR_BAL, 0) <> 0
      AND l.MATURITY_DATE >= l_report_date;

    GET DIAGNOSTICS l_row_cnt = ROW_COUNT;

    CALL TEMP.PR_PROCEDURE_LOG(
          'FORMAT'
        , 'TEMP.PR_EXTRACT_LOAN_RISK_DTL'
        , 'INFO'
        , 'TMP_LOAN_BASE inserted rows=' || CHAR(l_row_cnt)
    );

    ----------------------------------------------------------------
    -- Temporary table: exchange rate
    ----------------------------------------------------------------
    DECLARE GLOBAL TEMPORARY TABLE SESSION.TMP_FX_RATE
    (
        FROM_CCY       VARCHAR(3)    NOT NULL,
        TO_CCY         VARCHAR(3)    NOT NULL,
        RATE_DATE      DATE          NOT NULL,
        EXCH_RATE      DECIMAL(18,8) NOT NULL
    )
    ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE;

    INSERT INTO SESSION.TMP_FX_RATE
    SELECT
          f.FROM_CCY
        , f.TO_CCY
        , f.RATE_DATE
        , f.EXCH_RATE
    FROM INTERFACE.FX_RATE f
    WHERE f.RATE_DATE = l_report_date
      AND f.TO_CCY = l_reporting_ccy;

    ----------------------------------------------------------------
    -- Temporary table: guarantee enrich
    ----------------------------------------------------------------
    DECLARE GLOBAL TEMPORARY TABLE SESSION.TMP_GUARANTEE
    (
        DEAL_NUM                DECIMAL(20,0) NOT NULL,
        DEAL_SUB_NUM            DECIMAL(10,0) NOT NULL,
        GUARANTEE_TYPE          VARCHAR(20),
        GUARANTEE_VALUE         DECIMAL(18,2),
        GUARANTEE_VALUE_RPT     DECIMAL(18,2),
        GUARANTEE_CCY           VARCHAR(3)
    )
    ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE;

    INSERT INTO SESSION.TMP_GUARANTEE
    SELECT
          g.DEAL_NUM
        , g.DEAL_SUB_NUM
        , g.GUARANTEE_TYPE
        , g.GUARANTEE_AMT
        , CASE
              WHEN g.GUARANTEE_CCY = l_reporting_ccy
                   THEN g.GUARANTEE_AMT
              ELSE ROUND(g.GUARANTEE_AMT * COALESCE(fx.EXCH_RATE, 1), 2)
          END AS GUARANTEE_VALUE_RPT
        , g.GUARANTEE_CCY
    FROM INTERFACE.GUARANTEE_DEAL g
    LEFT JOIN SESSION.TMP_FX_RATE fx
           ON g.GUARANTEE_CCY = fx.FROM_CCY
          AND fx.TO_CCY       = l_reporting_ccy
          AND fx.RATE_DATE    = l_report_date
    WHERE g.ACTV_FLAG = 'A';

    ----------------------------------------------------------------
    -- Temporary table: charge / fee detail
    ----------------------------------------------------------------
    DECLARE GLOBAL TEMPORARY TABLE SESSION.TMP_CHARGE
    (
        DEAL_NUM                DECIMAL(20,0) NOT NULL,
        DEAL_SUB_NUM            DECIMAL(10,0) NOT NULL,
        CHARGE_AMT              DECIMAL(18,2),
        CHARGE_AMT_RPT          DECIMAL(18,2)
    )
    ON COMMIT PRESERVE ROWS NOT LOGGED WITH REPLACE;

    INSERT INTO SESSION.TMP_CHARGE
    SELECT
          c.DEAL_NUM
        , c.DEAL_SUB_NUM
        , SUM(COALESCE(c.CHARGE_AMT, 0)) AS CHARGE_AMT
        , SUM(
            CASE
                WHEN c.CHARGE_CCY = l_reporting_ccy
                     THEN COALESCE(c.CHARGE_AMT, 0)
                ELSE ROUND(COALESCE(c.CHARGE_AMT, 0) * COALESCE(fx.EXCH_RATE, 1), 2)
            END
          ) AS CHARGE_AMT_RPT
    FROM INTERFACE.SEC_CHARGE_DEAL c
    LEFT JOIN SESSION.TMP_FX_RATE fx
           ON c.CHARGE_CCY = fx.FROM_CCY
          AND fx.TO_CCY    = l_reporting_ccy
          AND fx.RATE_DATE = l_report_date
    WHERE c.CHARGE_TYPE IN ('BROKER', 'TXN', 'COMM')
    GROUP BY c.DEAL_NUM, c.DEAL_SUB_NUM;

    ----------------------------------------------------------------
    -- Main insert into report table
    ----------------------------------------------------------------
    INSERT INTO RPT.RPT_LOAN_RISK_DTL
    (
        BATCH_ID,
        REPORT_DATE,
        CUST_NUM,
        ACCT_NUM,
        DEAL_NUM,
        DEAL_SUB_NUM,
        DEAL_TYPE,
        COUNTRY_CODE,
        SOURCE_SYSTEM_ID,
        LOAN_CCY,
        CURR_BAL,
        CURR_BAL_RPT,
        BREAK_INT_AMT,
        BREAK_INT_AMT_RPT,
        GUARANTEE_IND,
        GUARANTEE_TYPE,
        GUARANTEE_VALUE,
        GUARANTEE_VALUE_RPT,
        RISK_RATING,
        PD,
        LGD,
        EXPECTED_LOSS,
        EXPECTED_LOSS_RPT,
        CHARGE_AMT,
        CHARGE_AMT_RPT,
        MATURITY_DATE,
        LOAD_TS
    )
    SELECT
          l_batch_id
        , l_report_date
        , b.CUST_NUM
        , b.ACCT_NUM
        , b.DEAL_NUM
        , b.DEAL_SUB_NUM
        , b.DEAL_TYPE
        , b.COUNTRY_CODE
        , b.SOURCE_SYSTEM_ID
        , b.LOAN_CCY
        , b.CURR_BAL
        , CASE
              WHEN b.LOAN_CCY = l_reporting_ccy
                   THEN b.CURR_BAL
              ELSE ROUND(b.CURR_BAL * COALESCE(fx.EXCH_RATE, 1), 2)
          END AS CURR_BAL_RPT
        , b.BREAK_INT_AMT
        , CASE
              WHEN b.LOAN_CCY = l_reporting_ccy
                   THEN b.BREAK_INT_AMT
              ELSE ROUND(b.BREAK_INT_AMT * COALESCE(fx.EXCH_RATE, 1), 2)
          END AS BREAK_INT_AMT_RPT
        , b.GUARANTEE_IND
        , g.GUARANTEE_TYPE
        , g.GUARANTEE_VALUE
        , g.GUARANTEE_VALUE_RPT
        , b.RISK_RATING
        , b.PD
        , b.LGD
        , ROUND(COALESCE(b.CURR_BAL,0) * COALESCE(b.PD,0) * COALESCE(b.LGD,0), 2) AS EXPECTED_LOSS
        , ROUND(
            CASE
                WHEN b.LOAN_CCY = l_reporting_ccy
                     THEN COALESCE(b.CURR_BAL,0) * COALESCE(b.PD,0) * COALESCE(b.LGD,0)
                ELSE COALESCE(b.CURR_BAL,0) * COALESCE(b.PD,0) * COALESCE(b.LGD,0) * COALESCE(fx.EXCH_RATE,1)
            END
          , 2) AS EXPECTED_LOSS_RPT
        , COALESCE(c.CHARGE_AMT, 0)
        , COALESCE(c.CHARGE_AMT_RPT, 0)
        , b.MATURITY_DATE
        , CURRENT TIMESTAMP
    FROM SESSION.TMP_LOAN_BASE b
    LEFT JOIN SESSION.TMP_FX_RATE fx
           ON b.LOAN_CCY   = fx.FROM_CCY
          AND fx.TO_CCY    = l_reporting_ccy
          AND fx.RATE_DATE = l_report_date
    LEFT JOIN SESSION.TMP_GUARANTEE g
           ON b.DEAL_NUM     = g.DEAL_NUM
          AND b.DEAL_SUB_NUM = g.DEAL_SUB_NUM
    LEFT JOIN SESSION.TMP_CHARGE c
           ON b.DEAL_NUM     = c.DEAL_NUM
          AND b.DEAL_SUB_NUM = c.DEAL_SUB_NUM;

    GET DIAGNOSTICS l_row_cnt = ROW_COUNT;

    CALL TEMP.PR_PROCEDURE_LOG(
          'FORMAT'
        , 'TEMP.PR_EXTRACT_LOAN_RISK_DTL'
        , 'INFO'
        , 'Main report inserted rows=' || CHAR(l_row_cnt)
    );

    ----------------------------------------------------------------
    -- Post update: enrich sector / group mapping
    ----------------------------------------------------------------
    UPDATE RPT.RPT_LOAN_RISK_DTL r
       SET (CUSTOMER_GROUP, INDUSTRY_CODE) =
           (
               SELECT
                     COALESCE(c.CUSTOMER_GROUP, 'UNKNOWN')
                   , COALESCE(c.INDUSTRY_CODE, 'UNKNOWN')
               FROM INTERFACE.CUST_PROFILE c
               WHERE c.CUST_NUM = r.CUST_NUM
               FETCH FIRST 1 ROW ONLY
           )
     WHERE EXISTS
           (
               SELECT 1
               FROM INTERFACE.CUST_PROFILE c
               WHERE c.CUST_NUM = r.CUST_NUM
           )
       AND r.BATCH_ID = l_batch_id;

    ----------------------------------------------------------------
    -- Optional logic controlled by config
    ----------------------------------------------------------------
    IF l_ctrl_flag = 'Y' THEN

        UPDATE RPT.RPT_LOAN_RISK_DTL r
           SET HIGH_RISK_FLAG =
               CASE
                   WHEN COALESCE(r.PD,0) >= 0.200000
                     OR COALESCE(r.LGD,0) >= 0.700000
                     OR COALESCE(r.EXPECTED_LOSS_RPT,0) >= 1000000
                   THEN 'Y'
                   ELSE 'N'
               END
         WHERE r.BATCH_ID = l_batch_id;

        CALL TEMP.PR_PROCEDURE_LOG(
              'FORMAT'
            , 'TEMP.PR_EXTRACT_LOAN_RISK_DTL'
            , 'INFO'
            , 'High risk flag updated'
        );

    END IF;

    ----------------------------------------------------------------
    -- Final commit
    ----------------------------------------------------------------
    COMMIT;

    CALL TEMP.PR_PROCEDURE_LOG(
          'FORMAT'
        , 'TEMP.PR_EXTRACT_LOAN_RISK_DTL'
        , 'INFO'
        , 'Procedure completed successfully. Batch=' || l_batch_id
    );

END
@
