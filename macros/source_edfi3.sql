{#- Created an is_deleted flag using deletes resources -#}
{% macro source_edfi3(resource, join_deletes=True) -%}

    {% if join_deletes %}
        select
            api_data.*,
            /* The reason to convert variant to a json string is because variants are case sensitive,
               but json strings are not. So this will get "id" in any case form (id, ID, iD, Id). */
            (to_json(deletes_data.v)):id is not null as is_deleted,
            coalesce(deletes_data.pull_timestamp, api_data.pull_timestamp) as last_modified_timestamp
        from {{ source('raw_edfi_3', resource) }} as api_data

            left join {{ source('raw_edfi_3', '_deletes') }} as deletes_data
            on (
                deletes_data.name = '{{ resource | lower }}'
                and api_data.tenant_code = deletes_data.tenant_code
                and api_data.api_year = deletes_data.api_year
                and api_data.v:id::string = replace((to_json(deletes_data.v)):id, '-')
            )

    {% else %}
        select *, false as is_deleted, pull_timestamp as last_modified_timestamp
        from {{ source('raw_edfi_3', resource) }}

    {% endif %}

{%- endmacro %}
