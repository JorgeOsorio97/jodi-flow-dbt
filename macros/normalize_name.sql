{% macro normalize_name(first_col, last_col, order='fwd') %}
(
    lower(regexp_replace(
        {% if order == 'fwd' %}
        coalesce({{ first_col }}, '') || coalesce({{ last_col }}, '')
        {% else %}
        coalesce({{ last_col }}, '') || coalesce({{ first_col }}, '')
        {% endif %},
        '[^a-zA-Z]', '', 'g'
    ))
)
{% endmacro %}

{% macro extract_wa_placeholder_name(column, min_len=8) %}
(
    case
        when {{ column }} like '~%'
             and length(regexp_replace(substr({{ column }}, 2), '[^a-zA-Z]', '', 'g')) >= {{ min_len }}
        then lower(regexp_replace(substr({{ column }}, 2), '[^a-zA-Z]', '', 'g'))
    end
)
{% endmacro %}
