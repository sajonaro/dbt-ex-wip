with derived_from_cust as (
    select
        *
    from {{ ref('int_cust_file') }}
),

keep_selected_and_reorder_columns   as (
    select
        Ref,
        Total_CD,
        Total_Retirement,
        Total_Check,
        Total_Equity,
        Total_AutoLoan,
        Total_Loan,
        Total_MoneyMarket,
        Total_Mortgage,
        Total_Savings
       
    from derived_from_cust
),

final as (
    select *
    from keep_selected_and_reorder_columns
)

select * from final