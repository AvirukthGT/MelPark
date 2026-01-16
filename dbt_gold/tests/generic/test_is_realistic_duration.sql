{% test is_realistic_duration(model, column_name) %}

    select *
    from {{ model }}
    where {{ column_name }} > (48 * 60) -- More than 48 hours (in minutes)

{% endtest %}