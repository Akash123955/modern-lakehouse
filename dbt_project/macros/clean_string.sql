/*
    Macro: clean_string
    Description: Trims leading/trailing whitespace and uppercases a string column.
                 Used in the silver layer to standardize string values coming from
                 raw source data that may have inconsistent casing or padding.

    Usage:
        {{ clean_string('column_name') }}

    Example:
        SELECT {{ clean_string('borough') }} AS borough FROM ...
        -- Equivalent to: SELECT TRIM(UPPER(borough)) AS borough FROM ...

    Arguments:
        column_name (str): The name of the column to clean.

    Returns:
        SQL expression: TRIM(UPPER(<column_name>))
*/

{% macro clean_string(column_name) %}
    TRIM(UPPER({{ column_name }}))
{% endmacro %}
