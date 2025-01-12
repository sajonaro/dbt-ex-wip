{# 
    This is a macro to iterate over columns in a CTE to apply transformations 
#}
{% macro apply_transformation_to_all_cols_in_cte(cte, transformation) %}
{% if execute %}
    {%- set columns = [] -%}
    {%- set result = run_query("SELECT * FROM ({}) AS cte LIMIT 1".format(cte)) -%}
    {%- for col in result.columns -%}
       {%- do columns.append( transformation.replace('[col_name]', col.name) ) -%}
    {%- endfor -%}
    {{ columns | join(', ') }}
{% endif %}    
select 1 -- dummy SQL for parsing stage
{% endmacro %}
