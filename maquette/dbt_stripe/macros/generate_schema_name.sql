{#
  Par défaut dbt préfixe le schéma personnalisé par le schéma cible
  (ex. "staging_analytics"). On garde des noms de couches propres
  ("staging", "analytics"), identiques à ceux du schéma d'architecture.
  Référence : documentation dbt, macro "generate_schema_name".
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
