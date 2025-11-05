{% macro log_load_info(model_name) %}
  {# Run only during execution, not compile time #}
  {% if execute %}
      {% set count_query = "select count(*) from " ~ model_name %}
      {% set result = run_query(count_query) %}
      {% if result is not none %}
          {% set count_result = result.columns[0][0] %}
          {% do log("ℹModel `" ~ model_name ~ "` | Load type: " ~ ("Incremental" if is_incremental() else "Full") ~ " | Rows loaded: " ~ count_result, info=True) %}
      {% else %}
          {% do log("Model `" ~ model_name ~ "` could not retrieve row count (table may not exist).", info=True) %}
      {% endif %}
  {% else %}
      {% do log("Skipping log_load_info for `" ~ model_name ~ "` (compile phase).", info=True) %}
  {% endif %}
{% endmacro %}
