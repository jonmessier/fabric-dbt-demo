{#
    Fails for rows where the column is below zero.
    Usage:
        data_tests:
          - non_negative
#}
{% test non_negative(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} < 0

{% endtest %}
