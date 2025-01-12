with derived_from_detail_8 as (
    select
        *
    from {{ ref('int_detail_8') }}
),

aggregate_and_group_by_hhkey   as (
    select
        MAX(Age) as Age,
        MIN(Dt1) as Dt1,
        MAX(BranchNum) as BranchNum,
        SUM(Bal_Check) as Bal_Check,
        SUM(Bal_MoneyMarket) as Bal_MoneyMarket,
        SUM(Bal_Savings) as Bal_Savings,
        SUM(Bal_CD) as Bal_CD,
        SUM(Bal_Loan) as Bal_Loan,
        SUM(Bal_Retirement) as Bal_Retirement,
        SUM(Bal_Equity) as Bal_Equity,
        SUM(Bal_Mortgage) as Bal_Mortgage,
        SUM(Bal_AutoLoan) as Bal_AutoLoan,
        SUM(Deposit_Balance) as Deposit_Balance,
        SUM(Loan_Balance) as Loan_Balance,
        SUM(Nmc_balance) as Nmc_balance,
        SUM(Revenue) as Revenue,
        SUM(NUM_ACCTS) as NUM_ACCTS,
        SUM(Total_Check) as Total_Check,
        SUM(Total_MoneMarket) as Total_MoneyMarket,
        SUM(Total_Savings) as Total_Savings,
        SUM(Total_CD) as Total_CD,
        SUM(Total_Loan) as Total_Loan,
        SUM(Total_Retirement) as Total_Retirement,
        SUM(Total_Equity) as Total_Equity,
        SUM(Total_Mortgage) as Total_Mortgage,
        SUM(Total_AutoLoan) as Total_AutoLoan,
        SUM(Deposit_Count) as Deposit_Count,
        SUM(Loan_count) as Loan_count,
        SUM(Nmc_count) as Nmc_count,
        MAX(BillPay) as BillPay, 
        MAX(DirectDeposit) as DirectDeposit,
        MIN(Dt2) as Dt2,
        MIN(AllClosed) as AllClosed,
    from derived_from_detail_8
    group by HHKey
),

coalesce_columns as (
    select 
        Age,
        Dt1,
        BranchNum,
        Bal_Check,
        Bal_MoneyMarket,
        Bal_Savings,
        Bal_CD,
        Bal_Loan,
        Bal_Retirement,
        Bal_Equity,
        Bal_Mortgage,
        Bal_AutoLoan,
        Deposit_Balance,
        Loan_Balance,
        Nmc_balance,
        Revenue,
        NUM_ACCTS,
        {{ dbt_utils.coalesce('Total_Check', 0) }},
        {{ dbt_utils.coalesce('Total_MoneyMarket', 0) }},
        {{ dbt_utils.coalesce('Total_Savings', 0) }},
        {{ dbt_utils.coalesce('Total_CD', 0) }},
        {{ dbt_utils.coalesce('Total_Loan', 0) }},
        {{ dbt_utils.coalesce('Total_Retirement', 0) }},
        {{ dbt_utils.coalesce('Total_Equity', 0) }},
        {{ dbt_utils.coalesce('Total_Mortgage', 0) }},
        {{ dbt_utils.coalesce('Total_AutoLoan', 0) }},
        {{ dbt_utils.coalesce('Deposit_Count', 0) }},
        {{ dbt_utils.coalesce('Loan_count', 0) }},
        {{ dbt_utils.coalesce('Nmc_count', 0) }},
        BillPay, 
        DirectDeposit,
        Dt2,
        AllClosed,
    from aggregate_and_group_by_hhkey
),
normalize_age as (
    select
        {{ star_cte('coalesce_columns', ['Age']) }},
        case
            when [Age] < 18 then 0
            else MIN([Age], 99)
        end as Age
    from coalesce_columns
),
calculate_new_columns as (
    select
        *,
        case
            when coalesce([Dt1], 0) > 202212 then 1
            else 0
        end as New,
        case
            when coalesce([Dt1], 222222) > 201401 then 1
            else 0
        end as Yr10P,
        2024 - FLOOR([Dt1] / 100) as Tenure,
       [Deposit_Balance]+[Loan_Balance] as All$,
        case
            when coalesce([Dt1], 0) > 202112 then 1
            else 0
        end as New2,
        1-[AllClosed] as Current
    from normalize_age
),
calculate_2_more_columns as (
    select
    *,
    case
        when [Current] = 1 and [All$] < 0.02 then 1
        else 0
    end as Dormant,
    case
       when [AllClosed] = 1 and coalesce([Dt2], 0) >= 202301 then 1
       else 0
    end as Gone
    from calculate_new_columns
),
remove_some_columns as (
    select
        {{ star_cte('calculate_2_more_columns', ['ALL$', 'AllClosed', 'BranchNum', 'Dt1','Dt2','New2']) }}
    from calculate_2_more_columns

),
rename_hhkey_and_sort as (
    select
        {{ star_cte('remove_some_columns', ['HHKey']) }},
        [HHKey] as Ref
    from remove_some_columns
    order by Ref
)


final as (
    select *
    from rename_hhkey_and_sort
)

select * from final