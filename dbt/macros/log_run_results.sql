{#
    Writes one row per node of the finished run into audit.dbt_run_log, so warnings, errors and
    skips can be reviewed in the warehouse (and in a report) instead of only in the console.

    Called from on-run-end in dbt_project.yml. Rows are inserted in chunks of 500 because a single
    INSERT ... VALUES takes at most 1000 rows in T-SQL.
#}
{% macro log_run_results(results) %}
    {% if not execute or results | length == 0 %}
        {{ return('') }}
    {% endif %}

    {%- set audit_schema = generate_schema_name('audit', none) -%}
    {%- set rows = [] -%}
    {%- for result in results -%}
        {%- set node = result.node -%}
        {%- set message = (result.message or '') | string | replace("'", "''") -%}
        {%- set failures = result.failures if result.failures is not none else 'null' -%}
        {%- do rows.append(
            "('" ~ invocation_id ~ "', '" ~ target.name ~ "', '" ~ node.unique_id ~ "', '"
            ~ node.resource_type ~ "', '" ~ node.name ~ "', '" ~ result.status ~ "', "
            ~ failures ~ ", '" ~ message[:3900] ~ "', " ~ (result.execution_time | round(3)) ~ ", '"
            ~ run_started_at.strftime('%Y-%m-%d %H:%M:%S') ~ "')"
        ) -%}
    {%- endfor %}

    if not exists (select 1 from sys.schemas where name = '{{ audit_schema }}')
        exec('create schema [{{ audit_schema }}]');

    if object_id('[{{ audit_schema }}].[dbt_run_log]') is null
        create table [{{ audit_schema }}].[dbt_run_log] (
            invocation_id varchar(64),
            target_name varchar(30),
            unique_id varchar(300),
            resource_type varchar(30),
            node_name varchar(200),
            status varchar(20),
            failures int,
            message varchar(4000),
            execution_time_seconds decimal(12, 3),
            run_started_at datetime2(6)
        );

    {% for batch in rows | batch(500) %}
    insert into [{{ audit_schema }}].[dbt_run_log]
    values
        {{ batch | join(',\n        ') }};
    {% endfor %}
{% endmacro %}
