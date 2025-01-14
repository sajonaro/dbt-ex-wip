with derived_from_detail as (
    select *
    from {{ ref('int_detail') }}
),
-- rename columns, drop unnecessary columns, cast columns to correct types
keep_columns as (
    select
        Age,
        AmId,
        HHKey
    from derived_from_detail
),

transform_age as  (
    select 
        --if([Age]>=18 and [Age]<70,[Age]+200,[Age])
        case 
            when [Age] >= 18 and [Age] < 70 then [Age] + 200
            else  [Age] 
        end as Age,    
        AmId,
        HHKey
    from keep_columns
),

for_hoh  as (
    select *
    from {{ ref('int_for_hoh') }}
)

merged_with_for_hoh as  (
    select 
        transform_age.*,
        for_hoh.MxAge
    from transform_age
    left join for_hoh on transform_age.HHKey = for_hoh.HHKey 
),

filter_age_equals_mxage as (
    select *
    from merged_with_for_hoh
    where MxAge == Age 
),

dedup_by_amid as (
    {{ dbt_utils.deduplicate(
        relation='ranked_by_amid',
        partition_by='AmId',
        order_by='AmId, row_num'
    )}}
)

add_hoh_1 as (
    select * ,
        1 as Hoh
    from dedup_by_amid
)

final as (
    select *
    from add_hoh_1
)

select * from final