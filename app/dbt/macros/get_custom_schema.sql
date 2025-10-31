{#
This macro overrides the default schema generation behavior.
If a custom schema is defined for a model, it uses that schema name directly.
Otherwise, it falls back to the target schema.
This prevents dbt from prepending the target schema (e.g., 'main') to custom schema names.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema or '' -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}