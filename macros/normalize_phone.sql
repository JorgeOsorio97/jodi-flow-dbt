{% macro normalize_phone(column) %}
(
    case
        when length(regexp_replace(coalesce({{ column }}, ''), '[^0-9]', '', 'g')) >= 10
        then right(regexp_replace(coalesce({{ column }}, ''), '[^0-9]', '', 'g'), 10)
    end
)
{% endmacro %}

{% macro normalize_wa_phone(column) %}
(
    case
        when {{ column }} not like '~%'
             and length(regexp_replace(coalesce({{ column }}, ''), '[^0-9]', '', 'g')) >= 10
        then right(regexp_replace(coalesce({{ column }}, ''), '[^0-9]', '', 'g'), 10)
    end
)
{% endmacro %}
