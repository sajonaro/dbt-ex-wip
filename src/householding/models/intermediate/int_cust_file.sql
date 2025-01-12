with derived_from_detail_4 as (
    select
        *
    from {{ ref('int_detail_4') }}
),

remove_age   as (
    select
        {{ dbt_utils.star(from=ref('int_detail_4'), except=['Age']) }}
    from derived_from_detail_4
    group by HHKey
),

final as (
    select *
    from rename_hhkey_and_sort
)

select * from final