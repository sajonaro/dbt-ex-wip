with amd as (
    select *
    from {{ ref( 'AMI0103000324020100029315') }}
),

-- rename columns, drop unnecessary columns, cast columns to correct types
final as (
    select *
    from amd
)

select * from final