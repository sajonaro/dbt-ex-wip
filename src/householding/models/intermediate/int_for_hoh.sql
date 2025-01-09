with derived_from_detail as (
    select *
    from {{ ref('int_detail') }}
),
-- rename columns, drop unnecessary columns, cast columns to correct types

transform_age as (
    select 
    --if([Age]>=18 and [Age]<70,[Age]+200,[Age])
    case
        when [Age] >= 18 and [Age] < 70 then [Age] + 200 
        else  [Age] 
    end as Age,
    *
    from derived_from_detail
),

max_age_group_by_hhkey as  (
    
    select 
        HKey, max(Age) as MxAge
    from transform_age
    group by HHKey
),

final as (
    select *
    from max_age_group_by_hhkey
)

select * from final