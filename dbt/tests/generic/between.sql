{#
    Fails for rows where the column is outside [min_value, max_value]. Nulls pass.
    Usage:
        data_tests:
          - between:
              min_value: 0
              max_value: 1
#}
{% test between(model, column_name, min_value, max_value) %}

select *
from {{ model }}
where {{ column_name }} < {{ min_value }}
   or {{ column_name }} > {{ max_value }}

{% endtest %}
