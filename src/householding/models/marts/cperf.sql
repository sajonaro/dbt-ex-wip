with cust_5 as (
    select * from {{ ref('int_cust_file_5') }}

)

final as (
    select *
    from cust_5
)

select * from final