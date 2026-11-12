{#
    Use schema names exactly as configured (silver, gold, ref, audit, snapshots)
    instead of dbt's default "<target_schema>_<custom_schema>".

    The ci target is the exception: it prefixes every schema with the target schema
    (ci_pr123_silver, ci_pr123_gold) so pull-request builds never touch real tables.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name == 'ci' -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
