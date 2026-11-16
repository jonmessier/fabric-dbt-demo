{#
    Writes one row per node of the finished run into audit.dbt_run_log, so warnings, errors and
    skips can be reviewed in the warehouse (and in a report) instead of only in the console.

    Besides the status, each row says what the node is about, so an alert can explain a failing test
    instead of only naming it:
      parent_model  the model/seed/snapshot a test is on (short name). Generic tests: attached_node.
                    Singular tests: the last model/seed/snapshot in depends_on.nodes. Unit tests: the
                    tested model. Null for models, seeds and snapshots themselves.
      column_name   the tested column (generic tests), else null
      test_kind     not_null, relationships, between, ... for generic tests; 'singular'; 'unit'; null
      severity      the test's severity (warn / error), lower case
      declared_in   the file the node is declared in, with forward slashes
      description   the node's description (cut to 400 characters), null if empty
      failures_relation  where store_failures saved a test's failing rows ([db].[audit].[test_name])
      parent_relation    the table or view of parent_model ([db].[silver].[int_batch_summary])

    Called from on-run-end in dbt_project.yml. Rows are inserted in chunks of 500 because a single
    INSERT ... VALUES takes at most 1000 rows in T-SQL.

    The table predates the columns above, so they are added on the fly (nullable, only if missing).
    That DDL runs as its own statement first: SQL Server compiles a whole batch before running it,
    so an INSERT in the same batch as the ALTER would fail on the not-yet-existing columns.
#}
{% macro log_run_results(results) %}
    {% if not execute or results | length == 0 %}
        {{ return('') }}
    {% endif %}

    {%- set audit_schema = generate_schema_name('audit', none) -%}
    {%- set table = '[' ~ audit_schema ~ '].[dbt_run_log]' -%}
    {%- set added_columns = [
        ('parent_model', 'varchar(200)'),
        ('column_name', 'varchar(200)'),
        ('test_kind', 'varchar(100)'),
        ('severity', 'varchar(10)'),
        ('declared_in', 'varchar(400)'),
        ('description', 'varchar(500)'),
        ('failures_relation', 'varchar(400)'),
        ('parent_relation', 'varchar(400)'),
    ] -%}

    {%- set ddl -%}
    if not exists (select 1 from sys.schemas where name = '{{ audit_schema }}')
        exec('create schema [{{ audit_schema }}]');

    if object_id('{{ table }}') is null
        create table {{ table }} (
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
    {% for name, data_type in added_columns %}
    if not exists (select 1 from sys.columns where object_id = object_id('{{ table }}') and name = '{{ name }}')
        exec('alter table {{ table }} add [{{ name }}] {{ data_type }} null');
    {%- endfor %}
    {%- endset -%}
    {% do run_query(ddl) %}

    {%- set rows = [] -%}
    {%- for result in results -%}
        {%- set node = result.node -%}
        {%- set info = node_test_info(node) -%}
        {%- set failures = result.failures if result.failures is not none else 'null' -%}
        {%- do rows.append(
            "(" ~ sql_string(invocation_id) ~ ", " ~ sql_string(target.name) ~ ", "
            ~ sql_string(node.unique_id) ~ ", " ~ sql_string(node.resource_type) ~ ", "
            ~ sql_string(node.name) ~ ", " ~ sql_string(result.status) ~ ", " ~ failures ~ ", "
            ~ sql_string(result.message, 3900) ~ ", " ~ (result.execution_time | round(3)) ~ ", "
            ~ sql_string(run_started_at.strftime('%Y-%m-%d %H:%M:%S')) ~ ", "
            ~ sql_string(info.parent_model) ~ ", " ~ sql_string(info.column_name) ~ ", "
            ~ sql_string(info.test_kind) ~ ", " ~ sql_string(info.severity) ~ ", "
            ~ sql_string(info.declared_in, 400) ~ ", " ~ sql_string(info.description, 400) ~ ", "
            ~ sql_string(info.failures_relation, 400) ~ ", " ~ sql_string(info.parent_relation, 400) ~ ")"
        ) -%}
    {%- endfor %}

    {% for batch in rows | batch(500) %}
    insert into {{ table }} (
        invocation_id, target_name, unique_id, resource_type, node_name, status, failures, message,
        execution_time_seconds, run_started_at,
        parent_model, column_name, test_kind, severity, declared_in, description,
        failures_relation, parent_relation
    )
    values
        {{ batch | join(',\n        ') }};
    {% endfor %}
{% endmacro %}


{#
    What a node is about, for audit.dbt_run_log. Reads the node as a dict, because the node classes
    differ: generic tests have test_metadata, attached_node and column_name; singular tests have none
    of them; unit tests have tested_node_unique_id and no severity. The parent's table comes from the
    graph (the manifest), which every macro can read.
#}
{% macro node_test_info(node) %}
    {%- set d = node.to_dict() -%}
    {%- set resource_type = d.get('resource_type') -%}
    {%- set config = d.get('config') or {} -%}
    {%- set info = {'parent_model': none, 'column_name': none, 'test_kind': none, 'severity': none,
                    'failures_relation': none, 'parent_relation': none} -%}
    {%- set parent = {'id': none} -%}

    {%- if resource_type == 'test' -%}
        {%- set metadata = d.get('test_metadata') or {} -%}
        {%- if metadata.get('name') -%}
            {#- generic test: declared in YAML on a model/seed/snapshot (or a source) -#}
            {%- do parent.update({'id': d.get('attached_node')}) -%}
            {%- do info.update({'test_kind': metadata.get('name'), 'column_name': d.get('column_name')}) -%}
        {%- else -%}
            {#- singular test: the last model, seed or snapshot it refs -#}
            {%- set parents = [] -%}
            {%- for unique_id in (d.get('depends_on') or {}).get('nodes') or [] -%}
                {%- if unique_id.split('.')[0] in ('model', 'seed', 'snapshot') -%}
                    {%- do parents.append(unique_id) -%}
                {%- endif -%}
            {%- endfor -%}
            {%- do parent.update({'id': parents[-1] if parents else none}) -%}
            {%- do info.update({'test_kind': 'singular'}) -%}
        {%- endif -%}
        {#- With store_failures, a test's relation_name is the audit table holding its failing rows. -#}
        {%- do info.update({'severity': (config.get('severity') or '') | lower or none,
                            'failures_relation': d.get('relation_name')}) -%}
    {%- elif resource_type == 'unit_test' -%}
        {%- do parent.update({'id': d.get('tested_node_unique_id')}) -%}
        {%- do info.update({'test_kind': 'unit'}) -%}
    {%- endif -%}

    {%- if parent.id -%}
        {%- set parent_node = graph.nodes.get(parent.id) or graph.sources.get(parent.id) or {} -%}
        {%- do info.update({'parent_model': short_node_name(parent.id),
                            'parent_relation': parent_node.get('relation_name')}) -%}
    {%- endif -%}

    {%- set description = (d.get('description') or '') | trim -%}
    {%- do info.update({
        'declared_in': (d.get('original_file_path') or '') | replace('\\', '/') or none,
        'description': description[:400] if description else none,
    }) -%}
    {{ return(info) }}
{% endmacro %}


{# 'model.lpb.int_batch_summary' -> 'int_batch_summary'. Sources keep 'source.table'. #}
{% macro short_node_name(unique_id) %}
    {%- if not unique_id -%}
        {{ return(none) }}
    {%- endif -%}
    {%- set parts = unique_id.split('.') -%}
    {%- if parts[0] == 'source' and parts | length >= 4 -%}
        {{ return(parts[-2] ~ '.' ~ parts[-1]) }}
    {%- endif -%}
    {{ return(parts[2:] | join('.')) }}
{% endmacro %}


{# A T-SQL string literal ('it''s'), or null for none/empty. Cut to max_length characters. #}
{% macro sql_string(value, max_length=none) %}
    {%- if value is none -%}
        {{ return('null') }}
    {%- endif -%}
    {%- set text = value | string -%}
    {%- if text == '' -%}
        {{ return('null') }}
    {%- endif -%}
    {%- if max_length -%}
        {%- set text = text[:max_length] -%}
    {%- endif -%}
    {{ return("'" ~ text | replace("'", "''") ~ "'") }}
{% endmacro %}
