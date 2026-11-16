{#
    Drops the temporary relations dbt-fabric 1.10 leaves behind (docs/SPIKE.md item 10):

    * <test>__dbt_tmp_vw views: store_failures builds each audit table from a temporary view and
      never drops it, so every data test leaves one in the audit schema.
    * <unit test>__dbt_tmp tables: a unit test that errors keeps its temporary table, and the next
      run of that unit test then fails with "There is already an object named ...".

    Called from on-run-start (clears what a failed run left) and on-run-end (clears this run's
    views). Only schemas this project builds into are touched, so parallel builds into other
    schemas (CI) are left alone. Remove once dbt-fabric cleans up after itself.
#}
{% macro drop_dbt_tmp_relations() %}
    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {%- set schemas = [] -%}
    {%- for node in graph.nodes.values() -%}
        {%- if node.schema and node.schema not in schemas -%}
            {%- do schemas.append(node.schema) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if schemas | length == 0 -%}
        {{ return('') }}
    {%- endif -%}

    {% set find %}
        select
            case o.type when 'V' then 'view' else 'table' end as kind,
            s.name as schema_name,
            o.name as object_name
        from sys.objects as o
        inner join sys.schemas as s on o.schema_id = s.schema_id
        where o.type in ('U', 'V')
          and s.name in ('{{ schemas | join("', '") }}')
          and (o.name like '%[_][_]dbt[_]tmp' or o.name like '%[_][_]dbt[_]tmp[_]vw')
    {% endset %}
    {%- set leftovers = run_query(find) -%}

    {%- for row in leftovers.rows %}
    drop {{ row['kind'] }} if exists [{{ row['schema_name'] }}].[{{ row['object_name'] }}];
    {%- endfor %}
    {%- if leftovers.rows | length > 0 %}
    {{ log("Dropped " ~ (leftovers.rows | length) ~ " leftover __dbt_tmp relation(s)", info=false) }}
    {%- endif %}
{% endmacro %}
