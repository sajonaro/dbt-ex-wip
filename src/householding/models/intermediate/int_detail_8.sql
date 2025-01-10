with derived_from_detail_3 as (
    select
        *
    from {{ ref('int_detail_3') }}
),

remove_12_columns as (
    select Age,AllClosed,AmId,BillPay,BranchId,BranchNum,Cust_AttritionDate,Cust_Tenure,CustNum,CustStartDate,Deposit_Count,DirectDeposit,Dt1,Dt2
    from derived_from_detail_3
),
noethou as (
    select *
    from {{ ref('int_noe_thou')}}
),

left_joined_with_noethou_by_amid as (

    select
        remove_12_columns.*,
        noethou.Bal_AutoLoan,
        noethou.Bal_CD,
        noethou.Bal_Check,
        noethou.Bal_Equity,
        noethou.Bal_Loan,
        noethou.Bal_Mortgage,
        noethou.Bal_MoneyMarket,
        noethou.Bal_Retirement,
        noethou.Bal_Savings,
        noethou.Deposit_Balance,
        noethou.[Loan Balance]
    from remove_12_columns
    left join noethou on remove_12_columns.AmId = noethou.AmId

),

replace_age_branchnum as (

    select  {{ all_columns | except('Age', 'branchnum')  }},
        case
            when [Hoh] == 1 then [Age] 
            else  0 
        end as Age,
        case
             when [Hoh] == 1 then [BranchNum] 
            else  0 
        end as BranchNum
    from left_joined_with_noethou_by_amid
),

add_nmc_count as (
    select
        {{ all_columns | insert_after('Loan_count',
             { 'Nmc_count' : 'Loan_Count-Total_Mortgage' }) }}
    from replace_age_branchnum
),
add_nmc_balance as (
    elect
        {{ all_columns | insert_after('Loan_count',
             { 'Nmc_balance' : {{ dbt_utils.as_number('Loan_Balance') }} - {{ dbt_utils.as_number(dbt_utils.coalesce('Bal_Mortgage', 0)) }}}) }}
    from add_nmc_count
)


final as (
    select *
    from add_nmc_balance
)

select * from final