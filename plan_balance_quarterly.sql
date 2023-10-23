%sql
use newr_Transform;
select count(*) from(
select 
  coalesce(m.client_id, '-9999') as client_id, 
  coalesce(m.plan_number, '-9999') as plan_number, 
  coalesce(
    m.source_cycle_date, 
    current_date() -1
  ) as source_cycle_date, 
  coalesce(m.core_cash_value_amount, 0.00) as core_cash_value_amount, 
  coalesce(m.ee_cash_value_amount, 0.00) as ee_cash_value_amount, 
  coalesce(m.er_cash_value_amount, 0.00) as er_cash_value_amount, 
  coalesce(m.life_cash_value_amount, 0.00) as life_cash_value_amount, 
  coalesce(m.sdba_cash_value_amount, 0.00) as sdba_cash_value_amount, 
  coalesce(m.loan_cash_value_amt, 0.00) as loan_cash_value_amount, 
  coalesce(m.noncore_cash_value_amount, 0.00) as noncore_cash_value_amount, 
  coalesce(m.ytd_contributions, 0.00) as ytd_contributions, 
  coalesce(m.total_shares, 0.00) as total_shares, 
  cast(
    m.allocated_participant_count as INTEGER
  ) as allocated_participant_count, 
  cast(
    m.active_participant_count as INTEGER
  ) as active_participant_count, 
  cast(m.participant_count as INTEGER) as participant_count, 
  cast(
    (
      m.core_cash_value_amount + m.noncore_cash_value_amount
    ) as DECIMAL(17, 2)
  ) AS total_cash_amount, 
  cast(
    null as VARCHAR(36)
  ) as plan_key 
from 
  (
    select 
      T.source_cycle_date, 
      T.client_id, 
      T.plan_number, 
      cast(
        T.core_cash_value_amount as DECIMAL(17, 2)
      ) as core_cash_value_amount, 
      cast(
        T.ee_cash_value_amount as DECIMAL(17, 2)
      ) as ee_cash_value_amount, 
      cast(
        T.er_cash_value_amount as DECIMAL(17, 2)
      ) as er_cash_value_amount, 
      cast(
        coalesce(t3.life_cash_value_amount, 0.00) as DECIMAL(13, 2)
      ) as life_cash_value_amount, 
      cast(
        T.sdba_cash_value_amount as DECIMAL(17, 2)
      ) as sdba_cash_value_amount, 
      cast(
        T.ytd_contributions as DECIMAL(17, 2)
      ) as ytd_contributions, 
      cast(
        T.loan_cash_value_amt as DECIMAL(17, 2)
      ) as loan_cash_value_amt, 
      cast(
        (
          coalesce(t3.life_cash_value_amount, 0.00) + coalesce(T.sdba_cash_value_amount, 0.00) + coalesce(T.loan_cash_value_amt, 0.00)
        ) as DECIMAL(13, 2)
      ) as noncore_cash_value_amount, 
      cast(
        coalesce(T.total_shares, 0.00) as DECIMAL(17, 4)
      ) as total_shares, 
      t5.participant_count, 
      t5.allocated_participant_count, 
      t5.active_participant_count 
    from 
      (
        SELECT
          coalesce(t1.client_id, t4.client_id) as client_id,
          coalesce(t1.plan_number, t4.plan_number) as plan_number,
          coalesce(t1.source_cycle_date, t4.source_cycle_date) as source_cycle_date,
          t1.core_cash_value_amount,
          t1.ee_cash_value_amount,
          t1.er_cash_value_amount,
          t1.sdba_cash_value_amount,
          t1.ytd_contributions,
          t1.total_shares,
          t4.loan_cash_value_amt
          from
          (
        select 
          p1.client_id, 
          p1.plan_number, 
          p1.source_cycle_date, 
          sum(p1.core_cash_value_amount) as core_cash_value_amount, 
          sum(
            coalesce(ee_cash_value_amount, 0.00)
          ) as ee_cash_value_amount, 
          sum(
            coalesce(er_cash_value_amount, 0.00)
          ) as er_cash_value_amount, 
          sum(sdba_cash_value_amount) as sdba_cash_value_amount, 
          sum(ytd_contributions) as ytd_contributions, 
          sum(total_shares) as total_shares 
        from 
          (
            select 
              pcb.client_id, 
              pcb.plan_number, 
              pcb.source_cycle_date, 
              case when pcb.fund_iv in ('70', '90', '91') 
              and pcb.client_id <> 'NG' then 0.0 else coalesce(pcb.cash_value_amount, 0.00) end as core_cash_value_amount, 
              coalesce(pcb.ytd_contributions, 0.00) as ytd_contributions, 
              case when money_type_description = 'EE' then cash_value_amount end as ee_cash_value_amount, 
              case when money_type_description = 'ER' then cash_value_amount end as er_cash_value_amount, 
              coalesce(
                pcb.sdba_cash_value_amount, 0.00
              ) as sdba_cash_value_amount, 
              coalesce(pcb.total_shares, 0.00) as total_shares 
            from 
              participant_core_balance_monthly pcb
          ) p1 
        group by 
          p1.client_id, 
          p1.plan_number, 
          p1.source_cycle_date
      ) t1 
      
      full outer join (
        select 
          k.client_id, 
          k.plan_number, 
          k.source_cycle_date, 
          sum(
            coalesce(k.loan_cash_value_amt, 0.00)
          ) as loan_cash_value_amt 
        from 
          (
            select 
              pl.client_id, 
              pl.plan_number, 
              pl.source_cycle_date, 
              case when pl.source_system = 'PREMIER' THEN outstanding_principal_balance ELSE loan_balance end as loan_cash_value_amt 
            from 
              participant_loan pl
          ) k 
          where k.loan_cash_value_amt > 0
        group by 
          k.plan_number, 
          k.source_cycle_date, 
          k.client_id
      ) t4 on coalesce(t1.client_id, -9999) = coalesce(t4.client_id, -9999) 
      and coalesce(t1.plan_number, -9999) = coalesce(t4.plan_number, -9999) 
      and coalesce(
        t1.source_cycle_date, 
        current_date() -1
      ) = coalesce(
        t4.source_cycle_date, 
        current_date() -1
      ) ) T

      left outer join (
        select 
          pff.plan_number, 
          sum(
            coalesce(pff.cash_value_amt, 0.00)
          ) as life_cash_value_amount, 
          pff.valuation_date 
        from 
          participant_life_fund_monthly pff 
        group by 
          pff.plan_number, 
          pff.valuation_date
      ) t3 on coalesce(T.plan_number, -9999) = coalesce(t3.plan_number, -9999) 
      and coalesce(
        T.source_cycle_date, 
        current_date() -1
      ) = coalesce(
        t3.valuation_date, 
        current_date() -1
      ) 

      left outer join (
        select 
          plan_number, 
          client_id, 
          source_cycle_date, 
          count(
            distinct (
              case when source_system = 'VRP-PB' 
              OR source_system = 'VRP_SP' then case when ACCOUNT_TYPE_CODE = 'ALLOC' then participant_id end when source_system = 'PREMIER' then participant_id end
            )
          ) as allocated_participant_count, 
          count(
            distinct (
              case when source_system = 'VRP-PB' 
              or source_system = 'VRP-SP' then case when PARTICIPANT_STATUS_code <= 16 then participant_id end when source_system = 'PREMIER' then case when PARTICIPANT_STATUS_code = 'A' then participant_id end end
            )
          ) as Active_participant_count, 
          count(
            distinct (participant_id)
          ) as participant_count 
        from 
          participant 
        group by 
          plan_number, 
          client_id, 
          source_cycle_date
      ) as t5 On coalesce(T.plan_number, -9999) = coalesce(t5.plan_number, -9999) 
      and T.source_cycle_date = t5.source_cycle_date
  ) m 
where 
  month (m.source_cycle_date) in (3, 9, 6, 12) )
