select AA.* from (
SELECT COALESCE(m1.client_id, '-9999')                    AS client_id,
       COALESCE(NULLIF(m1.plan_number, ''), '-9999')      AS plan_number,
       COALESCE(m1.participant_id, '-9999')               AS participant_id,
       m1.retirement_account_id,
       COALESCE(m1.source_cycle_date, CURRENT_DATE() - 1) AS source_cycle_date,
       coalesce(m1.core_cash_value_amount, 0,00) as core_cash_value_amount,
       coalesce(m1.ytd_contributions, 0.00) as ytd_contributions,
       coalesce(m1.ee_cash_value_amount, 0.00) as ee_cash_value_amount,
       coalesce(m1.er_cash_value_amount, 0.00) as er_cash_value_amount,
       coalesce(m1.life_cash_value_amount_monthly, 0.00) as life_cash_value_amount_monthly,
       coalesce(m1.sdba_cash_value_amount, 0.00) as sdba_cash_value_amount,
       COALESCE(m1.loan_cash_value_amt, 0.00) AS loan_cash_value_amount,
       coalesce(m1.noncore_cash_value_amount, 0.00) as noncore_cash_value_amount,
       m1.life_valuation_date

        


FROM   (SELECT T.source_cycle_date,
               T.participant_id,
               T.client_id,
               T.plan_number,
               T.retirement_account_id,
               t5.valuation_date AS life_valuation_date,
               Cast(T.core_cash_value_amount AS DECIMAL(17, 2)) AS core_cash_value_amount,
               Cast(T.ee_cash_value_amount AS DECIMAL(17, 2)) AS ee_cash_value_amount,
               Cast(T.er_cash_value_amount AS DECIMAL(17, 2)) AS er_cash_value_amount,
               Cast(t5.life_cash_value_amount AS DECIMAL(13, 2)) AS life_cash_value_amount_monthly,
               Cast(T.sdba_cash_value_amount AS DECIMAL(17, 2)) AS sdba_cash_value_amount,
               Cast(T.loan_cash_value_amt AS DECIMAL(17, 2)) AS loan_cash_value_amt,
               Cast(( COALESCE(T.sdba_cash_value_amount, 0.00)
                      + COALESCE(T.loan_cash_value_amt, 0.00)
                      + COALESCE(t5.life_cash_value_amount, 0.00) ) AS DECIMAL(
                    13, 2)) AS  noncore_cash_value_amount,
               Cast(T.ytd_contributions AS DECIMAL(17, 2)) AS ytd_contributions
        FROM   (select coalesce(t1.participant_id, t4.participant_id) as participant_id,
                       coalesce(t1.client_id, t4.client_id) as client_id,
                       coalesce(t1.plan_number, t4.plan_number) as plan_number,
                       coalesce(t1.retirement_account_id, t4.retirement_account_id) as retirement_account_id,
                       t1.source_cycle_date,
                       t1.core_cash_value_amount,
                       t1.er_cash_value_amount,
                       t1.ee_cash_value_amount,
                       t1.sdba_cash_value_amount,
                       t1.ytd_contributions,
                       t4.loan_cash_value_amt 
                       FROM
                        (SELECT p1.participant_id,
                       p1.client_id,
                       p1.plan_number,
                       p1.retirement_account_id,
                       p1.source_cycle_date,
                       Sum(p1.core_cash_value_amount)            AS
                       core_cash_value_amount,
                       Sum(COALESCE(ee_cash_value_amount, 0.00)) AS
                       ee_cash_value_amount,
                       Sum(COALESCE(er_cash_value_amount, 0.00)) AS
                       er_cash_value_amount,
                       Sum(sdba_cash_value_amount)               AS
                       sdba_cash_value_amount,
                       Sum(p1.ytd_contributions)                 AS
                       ytd_contributions
                FROM   (SELECT pcb.participant_id,
                               pcb.client_id,
                               pcb.plan_number,
                               pcb.source_cycle_date,
                               pcb.retirement_account_id,
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
                               sdba_cash_value_amount,
                               COALESCE(pcb.ytd_contributions, 0.00)      AS
                               ytd_contributions
                        FROM   participant_core_balance pcb) p1
                GROUP  BY p1.participant_id,
                          p1.client_id,
                          p1.plan_number,
                          p1.retirement_account_id,
                          p1.source_cycle_date) t1
               FULL OUTER JOIN (SELECT
                                    k.participant_id,
                                    k.client_id,
                                    k.plan_number,
                                    k.retirement_account_id,
                                    Sum(COALESCE(k.loan_cash_value_amt, 0.00)) AS loan_cash_value_amt
                                FROM   (SELECT pl.participant_id,
                                               pl.plan_number,
                                               pl.client_id,
                                               pl.retirement_account_id,
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
                                          k.retirement_account_id) t4
                            ON COALESCE(t1.participant_id, -9999) =
                               COALESCE(t4.participant_id, -9999)
                               AND COALESCE(t1.plan_number, -9999) =
                                   COALESCE(t4.plan_number, -9999)
                                AND COALESCE(t1.retirement_account_id, -9999) =
                                   COALESCE(t4.retirement_account_id, -9999)
                               AND COALESCE(t1.client_id, -9999) =
                                   COALESCE(t4.client_id, -9999)) T
               LEFT OUTER JOIN (SELECT Sum(COALESCE(cash_value_amt, 0)) AS
                                       life_cash_value_amount,
                                       plan_number,
                                       participant_id,
                                       valuation_date
                                FROM   participant_life_fund_monthly
                                GROUP  BY plan_number,
                                          participant_id,
                                          valuation_date) AS t5
                            ON COALESCE(T.participant_id, -9999) =
                               COALESCE(t5.participant_id, -9999)
                               AND COALESCE(T.plan_number, -9999) =
                                   COALESCE(t5.plan_number, -9999)) m1) AA 
                                   
                                   LEFT OUTER JOIN (
                                       select participant_id, 
                                       plan_number,
                                       client_id,
                                       part_status_desc
                                       from participant 

                                   ) BB 
                                   ON 
                                    COALESCE(AA.participant_id, -9999) =
                                    COALESCE(BB.participant_id, -9999)
                                    AND COALESCE(AA.plan_number, -9999) =
                                    COALESCE(BB.plan_number, -9999)
                                    AND COALESCE(AA.client_id, -9999) =
                                    COALESCE(BB.client_id, -9999)
                                    where (AA.core_cash_value_amount == 0 and AA.life_cash_value_amount_monthly == 0 
                                    and AA.sdba_cash_value_amount == 0 and AA.loan_cash_value_amount == 0 
                                    and AA.noncore_cash_value_amount == 0 and BB.part_status_desc == "ACTIVE")
                                    or
                                    ((AA.core_cash_value_amount != 0 or AA.life_cash_value_amount_monthly != 0 
                                    and AA.sdba_cash_value_amount != 0 or AA.loan_cash_value_amount != 0 
                                    or AA.noncore_cash_value_amount != 0) and BB.part_status_desc == "ACTIVE")
                                    or 
                                    (AA.core_cash_value_amount != 0 and AA.life_cash_value_amount_monthly != 0 
                                    and AA.sdba_cash_value_amount != 0 and AA.loan_cash_value_amount != 0 
                                    and AA.noncore_cash_value_amount != 0 and BB.part_status_desc != "ACTIVE")
                                    or 
                                    ((AA.core_cash_value_amount != 0 or AA.life_cash_value_amount_monthly != 0 
                                    or AA.sdba_cash_value_amount != 0 or AA.loan_cash_value_amount != 0 
                                    or AA.noncore_cash_value_amount != 0) and BB.part_status_desc != "ACTIVE")