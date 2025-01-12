with derived_from_cust_4 as (
    select
        *
    from {{ ref('int_cust_file_4') }}
),
cust_file_3 as (
    select
        *
    from {{ ref('int_cust_file_3') }}
),
merged_with_cust_file_3 as (
    select
        derived_from_cust_4.*,
        cust_file_3.Total_AutoLoan,
        cust_file_3.Total_CD,
        cust_file_3.Total_Check,
        cust_file_3.Total_Equity,
        cust_file_3.Total_Loan,
        cust_file_3.Total_MoneyMarket,
        cust_file_3.Total_Mortgage,
        cust_file_3.Total_Retirement,
        cust_file_3.Total_Savings,
    from    derived_from_cust_4
    left join cust_file_3 on derived_from_cust_4.Ref = cust_file_3.Ref
),
cust_file_2 as (
    select
        *
    from {{ ref('int_cust_file_2') }}
),

merged_with_cust_file_2  as (
    select
        merged_with_cust_file_3.*,
        cust_file_2.Bal_AutoLoan,
        cust_file_2.Bal_CD,
        cust_file_2.Bal_Check,
        cust_file_2.Bal_Equity,
        cust_file_2.Bal_Loan,
        cust_file_2.Bal_MoneyMarket,
        cust_file_2.Bal_Mortgage,
        cust_file_2.Bal_Retirement,
        cust_file_2.Bal_Savings,
    from merged_with_cust_file_3
    left join cust_file_2 on merged_with_cust_file_3.Ref = cust_file_2.Ref
),


final as (
    select *
    from merged_with_cust_file_2
)

select * from final