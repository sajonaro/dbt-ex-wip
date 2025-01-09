with ami as (
    select *
    from {{ source('input_data', 'AMI0103000324020100029315') }}
),

-- rename columns, drop unnecessary columns, cast columns to correct types
final as (
    select *
    from ami
)

select * from final