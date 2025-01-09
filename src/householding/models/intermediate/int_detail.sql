with removed_20_columns_from_amd as (
    select
        AmId, CustNum, Age, CustStartDate,	Cust_AttritionDate,	AllClosed,	BranchId,	BranchNum,	Bal_Check,	Total_Check,	Bal_MoneyMarket,	Total_MoneyMarket,	Bal_Savings	,Total_Savings	,Bal_CD	,Total_CD,	Bal_Loan,	Total_Loan	,		,Bal_Retirement,	Total_Retirement,	Bal_Equity,	Total_Equity,	Bal_Mortgage,	Total_Mortgage,	Bal_AutoLoan,	Total_AutoLoan,	Deposit_Balance,	Deposit_Count,	Loan_Balance,	Loan_Count,	Est_Curr_Revenue_Total,	Est_Curr_Revenue_Segment,	Cust_Tenure	NUM_ACCTS,NUM_Products,Num_Services,BillPay	,DirectDeposit,		
    from {{ ref('stg_amd') }}
),

hh_key as (
    select *
    from {{ ref('stg_ami') }}
),

merged_with_hh_key as  (
    select 
        20_columns_from_amd.*,
        hh_key.HHKey
    from 20_columns_from_amd
    left join hh_key on 20_columns_from_amd.AmId = hh_key.AmId
),
-- reorder columns (HHkey after CustNum ), sort by AmId asc, convert dates
reordered_sorted_date_converted as (
    select 
        AmId, CustNum, HHKey, cast(CustStartDate as DATE), cast(Cust_AttritionDate as DATE), AllClosed, BranchId, BranchNum, Age, 
        Bal_Check, Total_Check, Bal_MoneyMarket, Total_MoneyMarket, Bal_Savings, Total_Savings, Bal_CD, Total_CD, 
        Bal_Loan, Total_Loan, Bal_Retirement, Total_Retirement, Bal_Equity, Total_Equity, Bal_Mortgage, Total_Mortgage, 
        Bal_AutoLoan, Total_AutoLoan, Deposit_Balance, Deposit_Count, Loan_Balance, Loan_Count, Est_Curr_Revenue_Total, 
        Est_Curr_Revenue_Segment, Cust_Tenure, NUM_ACCTS, NUM_Products, Num_Services, BillPay, DirectDeposit
    from merged_with_hh_key
    order by AmId 

),
more_columns_converted as (
    select 
        AmId, CustNum, HHKey, cast(CustStartDate as DATE) as CustStartDate,
        case 
            when CustStartDate is null then null
            else 100 * date_trunc('year', CustStartDate) + date_trunc('month', CustStartDate) 
        end as Dt1,
        case 
            when Cust_AttritionDate is null then null
            else 100 * date_trunc('year', Cust_AttritionDate) + date_trunc('month', Cust_AttritionDate) 
        end as Dt2,
        cast(Cust_AttritionDate as DATE),
        case 
            when SUBSTRING(AllClosed,1,1) is 'C' 1
            else 0
        end as AllClosed,    
        BranchId, BranchNum,
        case 
            when Age is null then 0 
            when Age < 18 then 0 
            when Age > 99 then 99 
        end as Age, 
        Bal_Check, Total_Check, Bal_MoneyMarket, Total_MoneyMarket, Bal_Savings, Total_Savings, Bal_CD, Total_CD, 
        Bal_Loan, Total_Loan, Bal_Retirement, Total_Retirement, Bal_Equity, Total_Equity, Bal_Mortgage, Total_Mortgage, 
        Bal_AutoLoan, Total_AutoLoan, Deposit_Balance, Deposit_Count, Loan_Balance, Loan_Count, Est_Curr_Revenue_Total, 
        Est_Curr_Revenue_Segment, Cust_Tenure, NUM_ACCTS, NUM_Products, Num_Services, 
        case
            when BillPay is 'Y' then 1 
            else 0 
        end as BillPay,
        case
            when DirectDeposit is 'Y' then 1 
            else 0 
        end as DirectDeposit,
        
    from reordered_sorted_date_converted
),
final as (
    select *
    from more_columns_converted
)


select * from final
