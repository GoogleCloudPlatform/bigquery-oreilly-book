{% macro cleanup_numeric_macro() %}

CREATE OR REPLACE FUNCTION {{target.schema}}.cleanup_numeric(x STRING) AS
(
  IF ( x != 'NULL' AND x != 'PrivacySuppressed',
       CAST(x as FLOAT64),
       NULL )
);

{% endmacro %}
