{% macro insert_audit_log(model_name=None) %}
  {# 1. Determine model name safely #}
  {% if model_name is not none %}
      {% set model = model_name %}
  {% elif this is defined %}
      {% set model = this.identifier %}
  {% else %}
      {% set model = 'unknown_model' %}
  {% endif %}

  {# 2. Ensure audit_log table exists before inserting #}
  {% set create_table_sql %}
      create table if not exists audit_log (
          id serial primary key,
          model_name varchar(255),
          load_type varchar(50),
          row_count integer,
          load_time timestamp default current_timestamp
      );
  {% endset %}
  {% do run_query(create_table_sql) %}

  {# 3. Get row count of target model #}
  {% if execute %}
      {% set count_query = "select count(*) from " ~ model %}
      {% set count_result = run_query(count_query).columns[0][0] %}
  {% else %}
      {% set count_result = 0 %}
  {% endif %}

  {# 4. Insert record into audit_log #}
  {% if execute %}
      {% set audit_sql %}
          insert into audit_log (model_name, load_type, row_count, load_time)
          values ('{{ model }}', '{{ "Incremental" if is_incremental() else "Full" }}', {{ count_result }}, current_timestamp);
      {% endset %}
      {% do run_query(audit_sql) %}
      {% do log("Audit log recorded for model: " ~ model ~ " | Rows: " ~ count_result, info=True) %}
  {% endif %}
{% endmacro %}
