with ami as (
    select *
    from {{ ref('stg_ami') }}
),

-- rename columns, drop unnecessary columns, cast columns to correct types
final as (
    select  *      
    from ami
)

select * from final