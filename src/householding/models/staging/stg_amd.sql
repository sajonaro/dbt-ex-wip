with amd as (
    select *
    from {{ ref('AMD0103000224020100029315') }}
),

-- rename columns, drop unnecessary columns, cast columns to correct types
final as (
    select *
    from amd
)

select * from final