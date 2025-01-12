with derived_from_cust as (
    select
        *
    from {{ ref('int_cust_file') }}
),

keep_selected_and_reorder_columns   as (
    select
        Bal_CD,
        Bal_Retirement,
        Bal_Check,
        Bal_Equity,
        Bal_AutoLoan,
        Bal_Loan,
        Bal_MoneyMarket,
        Bal_Mortgage,
        Bal_Savings,
        Ref
        
    from derived_from_cust
),

final as (
    select *
    from rename_hhkey_and_sort
)

select * from final