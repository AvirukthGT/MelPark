{% macro is_weekend(date_column) %}
    -- Returns 1 if Saturday or Sunday, else 0
    case 
        when dayofweek({{ date_column }}) in (1, 7) then true
        else false
    end
{% endmacro %}