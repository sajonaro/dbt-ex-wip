with derived_from_detail as (
    select
        *
    from {{ ref('int_detail') }}
),

hoh as (
    select *
    from {{ ref('int_hoh') }}
),

left_joined_with_hoh_by_amid_and_put_hoh_after_age as  (
    select 
        derived_from_detail.AmId, derived_from_detail.CustNum, derived_from_detail.HHKey, derived_from_detail.Age,
        case 
            when hoh.Hoh is null then 0
            else hoh.Hoh
        end as Hoh, 
        derived_from_detail.CustStartDate, derived_from_detail.Dt1, derived_from_detail.Dt2, derived_from_detail.Cust_AttritionDate,
        derived_from_detail.AllClosed,derived_from_detail.BranchId, derived_from_detail.BranchNum, derived_from_detail.Bal_Check,
        derived_from_detail.Total_Check,derived_from_detail.Bal_MoneyMarket, derived_from_detail.Total_MoneyMarket, derived_from_detail.Bal_Savings, 
        derived_from_detail.Total_Savings, derived_from_detail.Bal_CD, derived_from_detail.Total_CD, derived_from_detail.Bal_Loan, 
        derived_from_detail.Total_Loan, derived_from_detail.Bal_Retirement, derived_from_detail.Total_Retirement, 
        derived_from_detail.Bal_Equity, derived_from_detail.Total_Equity, derived_from_detail.Bal_Mortgage, 
        derived_from_detail.Total_Mortgage, derived_from_detail.Bal_AutoLoan, derived_from_detail.Total_AutoLoan, 
        derived_from_detail.Deposit_Balance, derived_from_detail.Deposit_Count, derived_from_detail.Loan_Balance, derived_from_detail.Loan_Count, 
        derived_from_detail.Est_Curr_Revenue_Total as Revenue, 
        derived_from_detail.Est_Curr_Revenue_Segment, derived_from_detail.Cust_Tenure, derived_from_detail.NUM_ACCTS, derived_from_detail.NUM_Products, 
        derived_from_detail.Num_Services, derived_from_detail.BillPay, derived_from_detail.DirectDeposit,   
      
    from derived_from_detail
    left join hoh on derived_from_detail.AmId = hoh.AmId
),

final as (
    select *
    from left_joined_with_hoh_by_amid_and_put_hoh_after_age
)

select * from final