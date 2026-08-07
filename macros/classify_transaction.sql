{% macro classify_transaction_value(amount_col) %}
    CASE 
       WHEN {{ amount_col}} >= 5000 THEN 'High Value'
       WHEN {{ amount_col}} >= 1000 THEN 'Medium Value'
       WHEN {{ amount_col}} > 0 THEN 'Low Value'
       ELSE 'Invalid/ Zero Amount'
    END 
{% endmacro %}       