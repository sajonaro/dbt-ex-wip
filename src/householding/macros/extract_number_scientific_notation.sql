--This macro can be used to extract a numerical value from a text string that contains a scientific notation, such as "1.23E+45"
{% macro extract_exponent(var) %}

  CASE
    WHEN {{ var }} LIKE '%E%' THEN CAST(SUBSTR({{ var }}, INSTR({{ var }}, 'E') + 1) AS NUMBER)
    ELSE 0
  END

{% endmacro %}

--  return the position of the character 'E' in the Var column if it exists, otherwise it will return 0.
{% macro find_exponent(var) %}

  CASE
    WHEN {{ var }} IS NOT NULL AND {{ var }} != '' THEN INSTR({{ var }}, 'E')
    ELSE 0
  END

{% endmacro %}

--This macro can be used to extract a numerical value from a text string that contains a scientific notation
{% macro parse_scientific_notation(colname) %}

  CASE
    WHEN {{ find_exponent(colname) }} = 0 THEN {{ colname }}
    WHEN {{ extract_exponent(colname) }} > 0 THEN CAST(SUBSTR({{ colname }}, 1, {{ p }} - 1) AS NUMBER) * POWER(10, {{ e }})
    ELSE CAST(SUBSTR({{ colname }}, 1, {{ p }} - 1) AS NUMBER) / POWER(10, ABS({{ e }}))
  END

{% endmacro %}

--This macro can be used to round a number to 3 decimal places
{% macro round_to_mill(var) %}

  CASE
    WHEN {{ var }} IS NULL OR {{ var }} = '' THEN 0
    ELSE ROUND({{ var }} * 0.001, 3)
  END

{% endmacro %}


-- loop through all columns in the table and apply the transformation
-- for example:
--      with transformed_data as (
--        select
--          {{ loop_over_columns('my_table', '[col_name] * 2') }}
--        from my_table
--      )
-- multiplies value in each row of each column by 2

-- table_name parameter is a valid table name in your dbt project,
-- transformation parameter is a string that contains the [col_name] placeholder.
{% macro loop_over_columns(table_name, transformation) %}

  {% for column in graph.nodes[table_name].columns %}
    {{ column.name }} as {{ transformation.replace('[col_name]', column.name) }},
  {% endfor %}

{% endmacro %}