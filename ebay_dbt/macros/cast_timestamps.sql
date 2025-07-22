{% macro to_timestamp_ntz(column_name) %}
    CASE
        -- If the column is already a proper timestamp
        WHEN IS_TIMESTAMP_NTZ({{ column_name }}) THEN {{ column_name }}
        
        -- If it's a string that can be converted
        WHEN TRY_TO_TIMESTAMP_NTZ({{ column_name }}) IS NOT NULL 
            THEN TRY_TO_TIMESTAMP_NTZ({{ column_name }})
            
        -- If it's a different timestamp type that needs conversion
        WHEN TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR({{ column_name }})) IS NOT NULL 
            THEN TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR({{ column_name }}))
            
        -- Fallback to NULL
        ELSE NULL
    END
{% endmacro %}