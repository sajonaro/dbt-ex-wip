{# 
    This is a macro to overcome limitation of dbt-utils.star not working with CTE 

#}
{% macro star_cte(cte, exclude=[]) %}
{% if execute %}
    {%- set columns = [] -%}
    {%- set result = run_query("SELECT * FROM ({}) AS cte LIMIT 1".format(cte)) -%}
    {%- for column in result.columns -%}
        {%- if column.name not in exclude -%}
            {%- do columns.append(column.name) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ columns | join(', ') }}
{% endif %}    
select 1 -- dummy SQL for parsing stage
{% endmacro %}
