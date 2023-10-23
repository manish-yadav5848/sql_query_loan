SELECT COALESCE(m1.client_id, '-9999')                    AS client_id,
       COALESCE(NULLIF(m1.plan_number, ''), '-9999')      AS plan_number,
       COALESCE(NULLIF(m1.div_sub_id, ''), '-9999')       AS div_sub_id,
       m1.core_cash_value_amount,
       m1.noncore_cash_value_amount,
       m1.sdba_cash_value_amount,
       COALESCE(m1.life_cash_value_amount, 0.00)          AS
       life_cash_value_amount_monthly,
       m1.life_valuation_date,
       COALESCE(m1.loan_cash_value_amt, 0.00)             AS
       loan_cash_value_amount,
       m1.ee_cash_value_amount,
       m1.er_cash_value_amount,
       COALESCE(m1.allocated_participant_count, 0)        AS
       allocated_participant_count,
       COALESCE(m1.active_participant_count, 0)           AS
       active_participant_count,
       COALESCE(m1.participant_count, 0)                  AS participant_count,
       m1.ytd_contributions,
       COALESCE(m1.source_cycle_date, CURRENT_DATE() - 1) AS source_cycle_date,
       Cast(NULL AS VARCHAR(36))                          AS plan_key
FROM   (SELECT T.source_cycle_date,
               T.client_id,
               T.div_sub_id,
               T. plan_number,
               t3.valuation_date
                      AS life_valuation_date,
               Cast(T.core_cash_value_amount AS DECIMAL(17, 2))
                      AS core_cash_value_amount,
               Cast(T.ee_cash_value_amount AS DECIMAL(17, 2))
                      AS ee_cash_value_amount,
               Cast(T.er_cash_value_amount AS DECIMAL(17, 2))
                      AS er_cash_value_amount,
               Cast(t3.life_cash_value_amount AS DECIMAL(13, 2))
                      AS life_cash_value_amount,
               Cast(T.sdba_cash_value_amount AS DECIMAL(17, 2))
                      AS sdba_cash_value_amount,
               Cast(T.loan_cash_value_amt AS DECIMAL(17, 2))
                      AS loan_cash_value_amt,
               Cast(( COALESCE(T.sdba_cash_value_amount, 0.00)
                      + COALESCE(T.loan_cash_value_amt, 0.00)
                      + COALESCE(t3.life_cash_value_amount, 0.00) ) AS DECIMAL(
                    13, 2))
                      AS
               noncore_cash_value_amount,
               Cast(c1.allocated_participant_count AS INTEGER)
                      AS allocated_participant_count,
               Cast(c1.active_participant_count AS INTEGER)
                      AS active_participant_count,
               Cast(c1.participant_count AS INTEGER)
                      AS participant_count,
               Cast(T.ytd_contributions AS DECIMAL(17, 2))
                      AS ytd_contributions
        FROM   (select 
                coalesce(t1.client_id, t4.client_id) as client_id,
                coalesce(t1.plan_number,t4.plan_number) as plan_number,
                coalesce(t1.div_sub_id, t4.div_sub_id) as div_sub_id,
                t1.source_cycle_date,
                t1.core_cash_value_amount,
                t1.ee_cash_value_amount,
                t1.er_cash_value_amount,
                t1.sdba_cash_value_amount,
                t1.ytd_contributions,
                t4.loan_cash_value_amt from
            (SELECT p1.client_id,
                       p1.plan_number,
                       p1.div_sub_id,
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
                FROM   (SELECT Trim(pcb.client_id)                        AS
                               client_id,
                               Trim(pcb.plan_number)                      AS
                               plan_number,
                               Trim(p.div_sub_id)                         AS
                               div_sub_id
                               ,
                               pcb.source_cycle_date,
                               COALESCE(pcb.cash_value_amount, 0.00)      AS
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
                        FROM   participant_core_balance pcb
                               LEFT OUTER JOIN participant p
                                            ON pcb.plan_number = p.plan_number
                                               AND pcb.client_id = p.client_id
                                               AND pcb.participant_id =
                                                   p.participant_id) p1
                GROUP  BY p1.client_id,
                          p1.plan_number,
                          p1.div_sub_id,
                          p1.source_cycle_date) t1
               FULL OUTER JOIN (SELECT
                                                   k.client_id,
                                       k.plan_number,
                                       k.div_sub_id,
                                       Sum(COALESCE(k.loan_cash_value_amt, 0.00)
                                                   ) AS
                                                   loan_cash_value_amt
                                FROM   (SELECT pl.client_id,
                                               pl.plan_number,
                                               pl.participant_id,
                                               COALESCE(NULLIF(pt.div_sub_id, ''
                                                        ),
                                               '-9999') AS
                                               div_sub_id,
                                               CASE
                                                 WHEN pl.source_system =
                                                      'PREMIER' THEN
                                                 outstanding_principal_balance
                                                 ELSE loan_balance
                                               END
                                               AS
                                               loan_cash_value_amt
                                        FROM   participant_loan pl
                                               LEFT OUTER JOIN participant pt
                                                            ON
                                               COALESCE(pt.client_id,
                                               '-9999')
                                               =
                                               COALESCE(pl.client_id,
                                               '-9999')
                                               AND
                                               COALESCE(pt.plan_number,
                                               '-9999') =
                                               COALESCE(pl.plan_number,
                                               '-9999')
                                                               AND
                                               COALESCE(pt.participant_id,
                                               '-9999') =
                                               COALESCE(pl.participant_id,
                                               '-9999')) k
                                               where k.loan_cash_value_amt > 0
                                GROUP  BY k.plan_number,
                                          k.div_sub_id,
                                          k.client_id) t4
                            ON COALESCE(t1.client_id, -9999) =
                               COALESCE(t4.client_id, -9999)
                               AND COALESCE(t1.plan_number, -9999) =
                                   COALESCE(t4.plan_number, -9999)
                               AND COALESCE(t1.div_sub_id, -9999) =
                                   COALESCE(t4.div_sub_id, -9999)) T
               LEFT OUTER JOIN (SELECT Sum(COALESCE(plf.cash_value_amt, 0)) AS
                                       life_cash_value_amount,
                                       plf.plan_number,
                                       pt.div_sub_id,
                                       plf.valuation_date
                                FROM   participant_life_fund_monthly plf
                                       LEFT OUTER JOIN (SELECT plan_number,
                                                               participant_id,
                                                               div_sub_id
                                                        FROM   participant
                                                        GROUP  BY plan_number,
                                                                  participant_id,
                                                                  div_sub_id) pt
                                      ON COALESCE(pt.plan_number, '-9999'
                                      ) =
                                      COALESCE(plf.plan_number,
                                      '-9999')
                                      AND COALESCE(pt.participant_id,
                                      '-9999')
                                      =
                                      COALESCE(plf.participant_id,
                                      '-9999'
                                      )
                                      GROUP  BY plf.plan_number,
                                      pt.div_sub_id,
                                      plf.valuation_date) AS t3
                                      ON COALESCE(T.plan_number, -9999) =
                                      COALESCE(t3.plan_number, -9999)
                                      AND COALESCE(T.div_sub_id, -9999) =
                                      COALESCE(t3.div_sub_id, -9999)
                  LEFT OUTER JOIN (SELECT plan_number,
                                      client_id,
                                      div_sub_id,
                                      Count(DISTINCT ( allocated_participant_count ))
                                      AS
                                      allocated_participant_count,
                                      Count(DISTINCT ( active_participant_count ))
                                      AS
                                      Active_participant_count,
                                      Count(DISTINCT ( participant_count ))
                                      AS
                                      participant_count
                                      FROM   (SELECT pt.plan_number,
                                      pt.client_id,
                                      COALESCE(NULLIF(pt.div_sub_id, ''),
                                      '-9999') AS
                                      div_sub_id,
                                      CASE
                                      WHEN source_system = 'VRP-PB'
                                      OR source_system = 'VRP_SP' THEN
                                      CASE
                                      WHEN account_type_code = 'ALLOC'
                                      THEN
                                      participant_id
                                      END
                                      WHEN source_system = 'PREMIER' THEN
                                      participant_id
                                      END
                                      AS
                                      allocated_participant_count,
                                      CASE
                                      WHEN source_system = 'VRP-PB'
                                      OR source_system = 'VRP-SP' THEN
                                      CASE
                                      WHEN participant_status_code <= 16
                                      THEN
                                      participant_id
                                      END
                                      WHEN source_system = 'PREMIER' THEN
                                      CASE
                                      WHEN participant_status_code = 'A'
                                      THEN
                                      participant_id
                                      END
                                      END
                                      AS
                                      Active_participant_count
                                      ,
                                      participant_id
                                      AS
                                      participant_count
                                      FROM   participant pt)
                                      GROUP  BY plan_number,
                                      div_sub_id,
                                      client_id) AS c1
                                      ON COALESCE(T.plan_number, -9999) =
                                      COALESCE(c1.plan_number, -9999)
                                      AND COALESCE(T.div_sub_id, -9999) =
                                      COALESCE(c1.div_sub_id, -9999)) m1
                                      WHERE  div_sub_id NOT IN ( '-9999', '' )
                                      ORDER  BY plan_number DESC,
                                                div_sub_id DESC 