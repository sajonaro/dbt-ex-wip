{#
    loop through all columns in the table and apply the transformation
    for example:
         with transformed_data as (
           select
             {{ loop_over_columns('my_table', '[col_name] * 2') }}
           from my_table
         )
    multiplies value in each row of each column by 2

    table_name parameter is a valid table name in your dbt project,
    transformation parameter is a string that contains the [col_name] placeholder.
#}
{% macro loop_over_columns(table_name, transformation) %}

  {% for column in graph.nodes[table_name].columns %}
    {{ column.name }} as {{ transformation.replace('[col_name]', column.name) }},
  {% endfor %}

{% endmacro %}