with detail3 as (
    select
        *
    from {{ ref('int_detail_3') }}
),

keep_selected_columns as (
    select AmId,Bal_AutoLoan,Bal_CD, Bal_Check,Bal_Equity,Bal_Loan,Bal_MoneyMarket,Bal_Mortgage,Bal_Retirement,Bal_Savings,Deposit_Balance,HHKey,Loan_Balance,Revenue
    from detail3
),


--iterate over each ech column and each row (check macro definitions)    
iterate_over_noe as  (
       select
         {{ loop_over_columns('keep_selected_columns',  'round_to_mill(parse_scientific_notation([col_name]))') }}
       from keep_selected_columns
     )


final as (
    select *
    from iterate_over_noe
)

select * from final