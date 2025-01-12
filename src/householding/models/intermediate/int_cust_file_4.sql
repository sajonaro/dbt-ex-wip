with derived_from_cust as (
    select
        *
    from {{ ref('int_cust_file') }}
),

keep_selected   as (
    select
        BillPay,
        Current,
        Deposit_Balance,
        Deposit_Count,
        DirectDeposit,
        Dormant,
        Gone,
        Loan_Balance,
        Loan_Count,
        New,
        Nmc_balance,
        Nmc_count,
        NUM_ACCTS,
        Ref,
        Revenue,
        Tenure,
        Yr10P       
    from derived_from_cust
),

reorder_columns   as (
    select
        BillPay,
        Current,
        Deposit_Balance,
        DirectDeposit,
        Gone,
        Loan_Balance,
        Nmc_count,
        Loan_Count,
        Nmc_balance,
        Ref,
        NUM_ACCTS,
        Deposit_Count,
        Revenue,
        New,
        Yr10P   
        Tenure,
        Dormant,
           
    from keep_selected
)

final as (
    select *
    from reorder_columns
)

select * from final