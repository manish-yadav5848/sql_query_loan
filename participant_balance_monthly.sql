SELECT COALESCE(m1.client_id, '-9999')                    AS client_id,
       COALESCE(m1.plan_number, '-9999')                  AS plan_number,
       COALESCE(m1.participant_id, '-9999')               AS participant_id,
       COALESCE(m1.source_cycle_date, CURRENT_DATE() - 1) AS source_cycle_date,
       coalesce(m1.core_cash_value_amount, 0.00) as core_cash_value_amount,
       coalesce(m1.ee_cash_value_amount, 0.00) as ee_cash_value_amount,
       coalesce(m1.er_cash_value_amount, 0.00) as er_cash_value_amount,
       coalesce(m1.life_cash_value_amount_monthly, 0.00) as life_cash_value_amount_monthly,
       m1.valuation_date  AS life_valuation_date,
       coalesce(m1.sdba_cash_value_amount, 0.00) as sdba_cash_value_amount,
       COALESCE(m1.loan_cash_value_amt, 0.00) AS loan_cash_value_amount,
       coalesce(m1.noncore_cash_value_amount, 0.00) as noncore_cash_value_amount,
       coalesce(m1.ytd_contributions, 0.00) as ytd_contributions
FROM   (SELECT T.source_cycle_date,
               T.participant_id,
               T.client_id,
               T.plan_number,
               Cast(T.total_shares AS DECIMAL(17, 4))
                      AS total_shares,
               Cast(T.core_cash_value_amount AS DECIMAL(17, 2))
                      AS core_cash_value_amount,
               Cast(T.ee_cash_value_amount AS DECIMAL(17, 2))
                      AS ee_cash_value_amount,
               Cast(T.er_cash_value_amount AS DECIMAL(17, 2))
                      AS er_cash_value_amount,
               Cast(T.ytd_contributions AS DECIMAL(17, 2))
                      AS ytd_contributions,
               Cast(COALESCE(t3.life_cash_value_amount, 0.00) AS DECIMAL(13, 2))
                      AS
               life_cash_value_amount_monthly,
               t3.valuation_date,
               Cast(T.sdba_cash_value_amount AS DECIMAL(17, 2))
                      AS sdba_cash_value_amount,
               Cast(T.loan_cash_value_amt AS DECIMAL(17, 2))
                      AS loan_cash_value_amt,
               Cast(( COALESCE(t3.life_cash_value_amount, 0.00)
                      + COALESCE(T.sdba_cash_value_amount, 0.00)
                      + COALESCE(T.loan_cash_value_amt, 0.00)
                      + COALESCE(T.core_cash_value_amount, 0.00) ) AS DECIMAL(
                    17, 2))
                      AS
               total_cash_amount,
               Cast(( COALESCE(t3.life_cash_value_amount, 0.00)
                      + COALESCE(T.sdba_cash_value_amount, 0.00)
                      + COALESCE(T.loan_cash_value_amt, 0.00) ) AS
                    DECIMAL(13, 2))
                      AS
               noncore_cash_value_amount
        FROM   ( SELECT
            coalesce(t1.participant_id, t4.participant_id) as participant_id,
            coalesce(t1.client_id, t4.client_id) as client_id,
            coalesce(t1.plan_number, t4.plan_number) as plan_number,
            coalesce(t1.source_cycle_date, t4.source_cycle_date) as source_cycle_date,
            t1.total_shares,
            t1.ytd_contributions,
            t1.core_cash_value_amount,
            t1.ee_cash_value_amount,
            t1.er_cash_value_amount,
            t1.sdba_cash_value_amount, 
            t4.loan_cash_value_amt FROM
            (SELECT p1.participant_id,
                       p1.client_id,
                       Sum(p1.total_shares)                         AS
                       total_shares,
                       p1.plan_number,
                       p1.source_cycle_date,
                       Sum(COALESCE(p1.ytd_contributions, 0.00))    AS
                       ytd_contributions,
                       Sum(p1.core_cash_value_amount)               AS
                       core_cash_value_amount,
                       Sum(COALESCE(p1.ee_cash_value_amount, 0.00)) AS
                       ee_cash_value_amount,
                       Sum(COALESCE(p1.er_cash_value_amount, 0.00)) AS
                       er_cash_value_amount,
                       Sum(p1.sdba_cash_value_amount)               AS
                       sdba_cash_value_amount
                FROM   (SELECT pcb.participant_id,
                               pcb.client_id,
                               pcb.plan_number,
                               pcb.total_shares,
                               pcb.source_cycle_date,
                               COALESCE(pcb.ytd_contributions, 0.00)      AS
                               ytd_contributions,
                               CASE
                                 WHEN pcb.fund_iv IN( '70', '90', '91' )
                                      AND pcb.client_id <> 'NG' THEN 0.0
                                 ELSE COALESCE(pcb.cash_value_amount, 0.00)
                               END                                        AS
                               core_cash_value_amount,
                               CASE
                                 WHEN money_type_description = 'EE' THEN
                                 cash_value_amount
                               END                                        AS
                               ee_cash_value_amount,
                               CASE
                                 WHEN money_type_description = 'ER' THEN
                                 cash_value_amount
                               END                                        AS
                               er_cash_value_amount,
                               COALESCE(pcb.sdba_cash_value_amount, 0.00) AS
                               sdba_cash_value_amount
                        FROM   participant_core_balance_monthly pcb) p1
                GROUP  BY p1.participant_id,
                          p1.client_id,
                          p1.plan_number,
                          p1.source_cycle_date) t1
                    
                FULL OUTER JOIN (SELECT k.participant_id,
                                       k.plan_number,
                                       k.source_cycle_date,
                                       k.client_id,
                                       Sum(COALESCE(k.loan_cash_value_amt, 0.00)
                                       ) AS
                       loan_cash_value_amt
                                FROM   (SELECT pl.participant_id,
                                               pl.plan_number,
                                               pl.source_cycle_date,
                                               pl.client_id,
                                               CASE
                                                 WHEN pl.source_system =
                                                      'PREMIER' THEN
                                                 outstanding_principal_balance
                                                 ELSE loan_balance
                                               END AS loan_cash_value_amt
                                        FROM   participant_loan pl) k
                                        where k.loan_cash_value_amt > 0
                                GROUP  BY k.client_id,
                                          k.participant_id,
                                          k.plan_number,
                                          k.source_cycle_date) t4
                            ON COALESCE(t1.client_id, -9999) =
                               COALESCE(t4.client_id, -9999)
                               AND COALESCE(t1.participant_id, -9999) =
                                   COALESCE(t4.participant_id, -9999)
                               AND COALESCE(t1.plan_number, -9999) =
                                   COALESCE(t4.plan_number, -9999)
                               AND COALESCE(t1.source_cycle_date,
                                   CURRENT_DATE() - 1) =
                                   COALESCE(t4.source_cycle_date,
                                   CURRENT_DATE() - 1)) T

               LEFT OUTER JOIN (SELECT 
                                pff.participant_id,
                                pff.plan_number,
                                Sum(COALESCE(pff.cash_value_amt, 0.00))AS life_cash_value_amount,
                                pff.valuation_date FROM  participant_life_fund_monthly pff
                                GROUP  BY pff.participant_id,
                  pff.plan_number,
                  pff.valuation_date) t3
                            ON COALESCE(T.participant_id, -9999) =
                               COALESCE(t3.participant_id, -9999)
                               AND COALESCE(T.plan_number, -9999) =
                                   COALESCE(t3.plan_number, -9999)
                               AND COALESCE(T.source_cycle_date,
                                   CURRENT_DATE() - 1) =
                                   COALESCE(t3.valuation_date,
                                   CURRENT_DATE() - 1)

               )
       m1 